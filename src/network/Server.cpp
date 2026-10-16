#include "network/Server.h"
#include "commands/CommandParser.h"
#include <fcntl.h>
#include <iostream>
#include <string>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cerrno>
#include <unordered_map>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <netinet/tcp.h>
#include <cstring>
#include <algorithm>
#include <cctype>
#include <sstream>
#include <filesystem>
#include <fstream>
#include <cstdio>
#include <algorithm>
#include <limits>
#include <exception>
#include <chrono>
#include <optional>

namespace {

bool isMutationCommand(
    const std::string& command
)
{
    std::istringstream stream(command);

    std::string operation;

    stream >> operation;

    std::transform(
        operation.begin(),
        operation.end(),
        operation.begin(),
        [](unsigned char character) {
            return static_cast<char>(
                std::toupper(character)
            );
        }
    );

    return
        operation == "SET" ||
        operation == "DELETE";
}

}
namespace forgedb::network {

    Server::Server(
        int port,
        const std::string& node_id,
        NodeRole role,
        const std::string& replica_host,
        int replica_port,
        const std::vector<forgedb::raft::RaftPeer>& raft_peers
    )
        : port_(port),
          node_id_(node_id),
          role_(role),
          replica_host_(replica_host),
          replica_port_(replica_port),
          raft_state_(
              node_id,
              "raft.meta"
          ),
          raft_log_(
              "raft.log"
          ),
          raft_proposer_(
              raft_state_,
              raft_log_
          ),
          raft_peers_(raft_peers),
          raft_leader_progress_(raft_peers_),
          raft_commit_state_(raft_log_),

          raft_apply_progress_(
              raft_log_,
              "raft.applied"
          ),

          raft_committed_applier_(
              raft_log_,
              raft_commit_state_,
              raft_apply_progress_,
              store_
          ),

          raft_election_loop_(
              node_id_,
              raft_state_,
              raft_log_,
              raft_peers_,
              election_timer_,
              std::chrono::milliseconds{50}
          ),
          raft_heartbeat_loop_(
              node_id_,
              raft_state_,
              raft_log_,
              raft_peers_,
              raft_leader_progress_,
              raft_commit_state_,
              std::chrono::milliseconds{250},
              [this]() {
                  applyCommittedOrTerminate();
              }
          )
    {
        replication_enabled_ =
            role_ == NodeRole::LEADER &&
            !replica_host_.empty() &&
            replica_port_ > 0;
    }
    void Server::applyCommittedOrTerminate()
    {
        try {
            raft_committed_applier_.applyCommitted();
        }
        catch (const std::exception& e) {
            std::cerr
                << "FATAL: Raft state-machine "
                << "application failed on "
                << node_id_
                << ": "
                << e.what()
                << "\n";

            /*
             * Fail-stop: do not continue
             * serving with an unhealthy
             * replicated state machine.
             */
            std::terminate();
        }
    }
    void Server::start()
    {
        if (role_ == NodeRole::RAFT) {

            /*
             * KeyValueStore has already recovered
             * its own WAL during construction.
             *
             * Now replay any committed Raft entries
             * not covered by lastApplied.
             *
             * No clients or Raft workers are
             * running yet.
             */
            applyCommittedOrTerminate();

            std::cout
                << node_id_
                << " recovered Raft state:"
                << " commitIndex="
                << raft_commit_state_.commitIndex()
                << " lastApplied="
                << raft_apply_progress_.lastApplied()
                << "\n";
        }
        if (!loadReplicationState()) {

            std::cerr
                << "Failed to load "
                << "replication state\n";

            return;
        }

        int server_fd = socket(AF_INET, SOCK_STREAM, 0);

        if (server_fd < 0) {
            std::cerr << "Failed to create socket" << std::endl;
            return;
        }

        int flags = fcntl(server_fd, F_GETFL, 0);

        if (flags < 0 || fcntl(server_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            std::cerr << "Failed to set server socket to non-blocking" << std::endl;
            close(server_fd);
            return;
        }

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_addr.s_addr = INADDR_ANY;
        server_address.sin_port = htons(port_);

        if (bind(server_fd,reinterpret_cast<sockaddr*>(&server_address),sizeof(server_address)) < 0) {

            std::cerr << "Failed to bind socket" << std::endl;
            close(server_fd);
            return;
        }

        if (listen(server_fd, 10) < 0) {
            std::cerr << "Failed to listen on socket" << std::endl;
            close(server_fd);
            return;
        }

        int epoll_fd = epoll_create1(0);

        if (epoll_fd < 0) {
            std::cerr << "Failed to create epoll instance" << std::endl;
            close(server_fd);
            return;
        }

        response_event_fd_ = eventfd(0, EFD_NONBLOCK);

        if (response_event_fd_ < 0) {
            std::cerr << "Failed to create eventfd" << std::endl;
            close(epoll_fd);
            close(server_fd);
            return;
        }

        epoll_event server_event{};

        server_event.events = EPOLLIN;
        server_event.data.fd = server_fd;

        if (epoll_ctl(epoll_fd,EPOLL_CTL_ADD,server_fd,&server_event) < 0) {

            std::cerr << "Failed to add server socket to epoll"<< std::endl;

            close(response_event_fd_);
            close(epoll_fd);
            close(server_fd);
            return;
        }

        epoll_event response_event{};

        response_event.events = EPOLLIN;
        response_event.data.fd = response_event_fd_;

        if (epoll_ctl(epoll_fd,EPOLL_CTL_ADD,response_event_fd_,&response_event) < 0) {

            std::cerr << "Failed to add response event to epoll"<< std::endl;

            close(response_event_fd_);
            close(epoll_fd);
            close(server_fd);
            return;
        }

        epoll_event events[10];

        std::cout << "ForgeDB server started on port " << port_ << std::endl;
        std::cout
            << "Node ID: "
            << node_id_
            << "\n";

        std::cout
            << "Raft term: "
            << raft_state_.currentTerm()
            << "\n";

        std::cout
            << "Node role: ";

        if (role_ == NodeRole::LEADER) {
            std::cout << "LEADER";
        }
        else if (
            role_ == NodeRole::FOLLOWER
        ) {
            std::cout << "FOLLOWER";
        }
        else if (
            role_ == NodeRole::RAFT
        ) {
            std::cout << "RAFT";
        }
        else {
            std::cout << "STANDALONE";
        }
        std::cout << "\n";

        if (
            role_ == NodeRole::RAFT
        )
        {
            std::cout
                << "Raft peers: "
                << raft_peers_.size()
                << "\n";

            for (
                const auto& peer :
                raft_peers_
            )
            {
                std::cout
                    << "  "
                    << peer.node_id
                    << " -> "
                    << peer.host
                    << ":"
                    << peer.port
                    << "\n";
            }
        }

        if (replication_enabled_) {

            std::cout
                << "Connecting to replica "
                << replica_host_
                << ":"
                << replica_port_
                << "...\n";

            if (
                replica_client_.connectTo(
                    replica_host_,
                    replica_port_
                )
            ) {

                std::cout
                    << "Replica connected\n";

                if (!replayReplicationLog()) {
                    std::cerr<< "Initial replication catch-up failed\n";
                }
            }
            else {

                std::cerr
                    << "Warning: replica unavailable\n";
            }
        }

        if (
            role_ == NodeRole::RAFT
        )
        {
            /*
             * Heartbeat loop starts first.
             *
             * It does nothing until this node
             * actually becomes Raft leader.
             */
            raft_heartbeat_loop_.start();

            raft_election_loop_.start();

            std::cout
                << "Raft election and heartbeat "
                << "loops started\n";
        }

        while (true) {

            int event_count = epoll_wait(epoll_fd,events,10,-1);

            if (event_count < 0) {
                std::cerr << "epoll_wait failed" << std::endl;
                break;
            }

            for (int i = 0; i < event_count; ++i) {
                if (events[i].data.fd == server_fd) {

                    while(true){
                        sockaddr_in client_address{};
                        socklen_t client_address_length = sizeof(client_address);

                        int client_fd = accept(server_fd,reinterpret_cast<sockaddr*>(&client_address),&client_address_length);
                        int flag = 1;
                        if (client_fd < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            std::cerr << "Failed to accept client" << std::endl;
                            break;
                        }
                        if (setsockopt(
                                client_fd,
                                IPPROTO_TCP,
                                TCP_NODELAY,
                                &flag,
                                sizeof(flag)) < 0) {

                            std::cerr << "Failed to enable TCP_NODELAY for client\n";
                        }

                        int client_flags = fcntl(client_fd, F_GETFL, 0);

                        if (client_flags < 0 || fcntl(client_fd, F_SETFL, client_flags | O_NONBLOCK) < 0) {
                            std::cerr << "Failed to set client socket to non-blocking" << std::endl;
                            close(client_fd);
                            continue;
                        }

                        epoll_event client_event{};

                        client_event.events = EPOLLIN;
                        client_event.data.fd = client_fd;

                        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event) < 0) {
                            std::cerr << "Failed to add client socket to epoll" << std::endl;
                            close(client_fd);
                            continue;
                        }
                        active_clients_.insert(client_fd);
                        uint64_t generation = next_generation_++;
                        clients_.emplace(client_fd, ClientState{client_fd, generation});
                    }
                }
                else if (events[i].data.fd == response_event_fd_) {

                    uint64_t value;

                    if (read(response_event_fd_, &value, sizeof(value)) < 0) {
                        std::cerr << "eventfd read failed: "
                                  << strerror(errno) << std::endl;
                    }

                    while (true) {

                        ClientResponse response;

                        {
                            std::lock_guard<std::mutex> lock(responseMutex_);

                            if (responseQueue_.empty()) {
                                break;
                            }

                            response = std::move(responseQueue_.front());
                            responseQueue_.pop();
                        }

                        auto it = clients_.find(response.client_fd);

                        if (it != clients_.end() && it->second.generation == response.generation && it->second.active) {
                            it->second.output_buffer += response.response;
                            enableWrite(epoll_fd, response.client_fd);
                        }
                    }
                }
                else {
                    int client_fd = events[i].data.fd;

                    if (events[i].events & EPOLLOUT) {
                        handleWrite(epoll_fd, client_fd);
                    }

                    if (!(events[i].events & EPOLLIN)) {
                        continue;
                    }

                    while(true){
                        char buffer[1024];

                        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

                        if (bytes_received > 0) {

                            buffer[bytes_received] = '\0';

                            clients_[client_fd].receive_buffer += buffer;

                            size_t newline_pos;

                            while ((newline_pos = clients_[client_fd].receive_buffer.find('\n')) != std::string::npos) {
                                std::string command = clients_[client_fd].receive_buffer.substr(0, newline_pos);
                                clients_[client_fd].receive_buffer.erase(0, newline_pos + 1);
                                enqueueCommand(client_fd, command);
                            }
                        }
                        else if (bytes_received == 0) {

                            epoll_ctl(epoll_fd,EPOLL_CTL_DEL,client_fd,nullptr);

                            close(client_fd);

                            active_clients_.erase(client_fd);
                            clients_.erase(client_fd);

                            break;
                        }
                        else {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            std::cerr << "recv failed: "<< errno<< " - "<< strerror(errno)<< std::endl;

                            epoll_ctl(epoll_fd,EPOLL_CTL_DEL,client_fd,nullptr);

                            close(client_fd);

                            active_clients_.erase(client_fd);
                            clients_.erase(client_fd);

                            break;

                        }
                    }
                }
            }
        }

        if (
            role_ == NodeRole::RAFT
        )
        {
            raft_election_loop_.stop();

            raft_heartbeat_loop_.stop();
        }

        close(server_fd);
    }

    ClientResponse Server::processCommand(
        int client_fd,
        const std::string& command
    )
    {
        if (
            command.rfind(
                "RAFT_VOTE ",
                0
            ) == 0
        ) {

            return processRaftVoteRequest(
                client_fd,
                command
            );
        }
        if (
            command.rfind(
                "REPL ",
                0
            ) == 0
        ) {

            if (
                role_ !=
                NodeRole::FOLLOWER
            ) {

                return ClientResponse{
                    client_fd,
                    0,
                    "REPL_ERR not follower\n"
                };
            }

            return processReplicationCommand(
                client_fd,
                command
            );
        }
        if (
            role_ == NodeRole::FOLLOWER &&
            isMutationCommand(command)
        ) {

            return ClientResponse{
                client_fd,
                0,
                "ERR read-only replica\n"
            };
        }
        if (
            role_ == NodeRole::RAFT &&
            isMutationCommand(command)
        ) {
            /*
             * Step 15A:
             * Validate mutation syntax and
             * require an actual Raft leader.
             */
            std::istringstream stream(command);

            std::string operation;
            std::string key;
            std::string value;
            std::string extra;

            stream >> operation;

            bool valid = false;

            if (operation == "SET") {
                valid =
                    static_cast<bool>(
                        stream >> key >> value
                    ) &&
                    !(stream >> extra);
            }
            else if (operation == "DELETE") {
                valid =
                    static_cast<bool>(
                        stream >> key
                    ) &&
                    !(stream >> extra);
            }

            if (!valid) {
                return ClientResponse{
                    client_fd,
                    0,
                    "ERR invalid Raft mutation\n"
                };
            }

            /*
             * Only the elected Raft leader
             * may eventually accept proposals.
             */
            if (
                raft_state_.role() !=
                forgedb::raft::RaftRole::LEADER
            ) {
                return ClientResponse{
                    client_fd,
                    0,
                    "ERR not Raft leader\n"
                };
            }
            std::optional<forgedb::raft::RaftLogEntry> proposal;

            try {
                /*
                 * Coordinate local proposals with
                 * incoming AppendEntries processing.
                 * Do not hold this lock while waiting
                 * for a majority.
                 */
                std::lock_guard<std::mutex> lock(raft_append_mutex_);

                const uint64_t index =
                    raft_proposer_.propose(command);

                proposal = raft_log_.entryAt(index);
            }
            catch (const std::exception& e) {
                std::cerr
                    << "Raft proposal failed: "
                    << e.what() << "\n";

                return ClientResponse{
                    client_fd,
                    0,
                    "ERR Raft proposal failed; outcome uncertain\n"
                };
            }

            if (!proposal.has_value()) {
                return ClientResponse{
                    client_fd,
                    0,
                    "ERR Raft proposal missing; outcome uncertain\n"
                };
            }

            /*
             * Bounded wait prevents unavailable
             * quorum from blocking indefinitely.
             */
            const auto deadline =
                std::chrono::steady_clock::now() +
                std::chrono::seconds(3);

            while (std::chrono::steady_clock::now() < deadline) {

                /*
                 * The proposing leader must still
                 * be leader in the same term.
                 */
                if (
                    raft_state_.role() !=
                        forgedb::raft::RaftRole::LEADER ||
                    raft_state_.currentTerm() != proposal->term
                ) {
                    return ClientResponse{
                        client_fd,
                        0,
                        "ERR Raft leadership changed; outcome uncertain\n"
                    };
                }

                const uint64_t committed =
                    raft_commit_state_.commitIndex();

                const uint64_t applied =
                    raft_apply_progress_.lastApplied();

                if (
                    committed >= proposal->index &&
                    applied >= proposal->index
                ) {
                    /*
                     * Confirm the same log entry
                     * occupies the original index.
                     */
                    auto actual =
                        raft_log_.entryAt(proposal->index);

                    if (
                        !actual.has_value() ||
                        actual->term != proposal->term ||
                        actual->command != proposal->command
                    ) {
                        return ClientResponse{
                            client_fd,
                            0,
                            "ERR Raft proposal changed; outcome uncertain\n"
                        };
                    }

                    if (
                        raft_state_.role() !=
                            forgedb::raft::RaftRole::LEADER ||
                        raft_state_.currentTerm() != proposal->term
                    ) {
                        return ClientResponse{
                            client_fd,
                            0,
                            "ERR Raft leadership changed; outcome uncertain\n"
                        };
                    }

                    return ClientResponse{
                        client_fd,
                        0,
                        "OK\n"
                    };
                }

                std::this_thread::sleep_for(
                    std::chrono::milliseconds(20)
                );
            }

            return ClientResponse{
                client_fd,
                0,
                "ERR Raft commit timeout; outcome uncertain\n"
            };

        }
        if (
            command == "RAFT_APPEND" ||
            command.rfind(
                "RAFT_APPEND ",
                0
            ) == 0
        ) {
            return processRaftAppendRequest(
                client_fd,
                command
            );
        }
        forgedb::commands::CommandParser parser;

        auto parsed = parser.parse(command);

        /*
         * Read-consistency rules will be
         * completed on Day 4.
         *
         * Do not expose potentially stale
         * Raft state through GET yet.
         */
        if (
            role_ == NodeRole::RAFT &&
            parsed.type ==
                forgedb::commands::CommandType::GET
        ) {
            return ClientResponse{
                client_fd,
                0,
                "ERR Raft reads not enabled yet\n"
            };
        }

        std::string response =
            commandHandler_.execute(parsed);

        bool successful =
            response.rfind(
                "ERR",
                0
            ) != 0;

        if (
            replication_enabled_ &&
            successful &&
            isMutationCommand(command)
        ) {

            if (
                !replicateWithReconnect(
                    command
                )
            ) {

                std::cerr
                    << "Replication unavailable for: "
                    << command
                    << "\n";
            }
        }

        return ClientResponse{
            client_fd,
            0,
            response
        };
    }

    ClientResponse Server::processRaftVoteRequest(
        int client_fd,
        const std::string& command
    )
    {
        auto request =
            forgedb::raft::
                decodeRequestVote(
                    command
                );

        if (!request.has_value()) {

            return ClientResponse{
                client_fd,
                0,
                "ERR invalid Raft vote request\n"
            };
        }

        auto [
            local_last_log_index,
            local_last_log_term
        ] =
            raft_log_.lastLogInfo();

        forgedb::raft::
            RequestVoteResponse response;

        try {

            response =
                raft_state_.handleRequestVote(
                    *request,
                    local_last_log_index,
                    local_last_log_term
                );
            if (
                response.vote_granted
            )
            {
                election_timer_.reset();

                std::cout
                    << "Election timer reset after "
                    << "granting vote to "
                    << request->candidate_id
                    << " in term "
                    << request->term
                    << "\n";
            }
        }
        catch (
            const std::exception& e
        ) {

            std::cerr
                << "RequestVote processing failed: "
                << e.what()
                << "\n";

            return ClientResponse{
                client_fd,
                0,
                "ERR Raft persistence failure\n"
            };
        }

        return ClientResponse{
            client_fd,
            0,
            forgedb::raft::
                encodeRequestVoteResponse(
                    response
                )
        };
    }

    ClientResponse Server::processReplicationCommand(
        int client_fd,
        const std::string& command
    )
    {
        std::istringstream stream(
            command
        );

        std::string marker;
        uint64_t sequence = 0;

        stream >> marker;
        stream >> sequence;

        if (
            marker != "REPL" ||
            stream.fail()
        ) {

            return ClientResponse{
                client_fd,
                0,
                "REPL_ERR invalid request\n"
            };
        }

        std::string replicated_command;

        std::getline(
            stream,
            replicated_command
        );

        if (
            !replicated_command.empty() &&
            replicated_command.front() == ' '
        ) {

            replicated_command.erase(
                0,
                1
            );
        }

        if (
            replicated_command.empty() ||
            !isMutationCommand(
                replicated_command
            )
        ) {

            return ClientResponse{
                client_fd,
                0,
                "REPL_ERR invalid mutation\n"
            };
        }

        std::lock_guard<std::mutex> lock(
            replicaApplyMutex_
        );

        /*
         * Already applied.
         *
         * This can happen if the leader sent
         * an entry successfully but lost the
         * acknowledgement and retried it.
         */
        if (
            sequence <=
            last_applied_replication_sequence_
        ) {

            return ClientResponse{
                client_fd,
                0,
                "REPL_OK " +
                    std::to_string(sequence) +
                    "\n"
            };
        }

        uint64_t expected_sequence =
            last_applied_replication_sequence_ +
            1;

        /*
         * Missing one or more entries.
         */
        if (
            sequence !=
            expected_sequence
        ) {

            return ClientResponse{
                client_fd,
                0,
                "REPL_ERR gap expected=" +
                    std::to_string(
                        expected_sequence
                    ) +
                    " got=" +
                    std::to_string(
                        sequence
                    ) +
                    "\n"
            };
        }

        forgedb::commands::CommandParser parser;

        auto parsed =
            parser.parse(
                replicated_command
            );

        std::string response =
            commandHandler_.execute(
                parsed
            );

        if (
            response.rfind(
                "ERR",
                0
            ) == 0
        ) {

            return ClientResponse{
                client_fd,
                0,
                "REPL_ERR apply failed\n"
            };
        }

        if (
            !persistReplicationState(
                sequence
            )
        ) {

            return ClientResponse{
                client_fd,
                0,
                "REPL_ERR persistence failed\n"
            };
        }

        last_applied_replication_sequence_ =
            sequence;

        return ClientResponse{
            client_fd,
            0,
            "REPL_OK " +
                std::to_string(sequence) +
                "\n"
        };
    }

    void Server::handleWrite(int epoll_fd, int client_fd)
    {
        auto it = clients_.find(client_fd);

        if (it == clients_.end()) {
            return;
        }

        std::string& output = it->second.output_buffer;
        if (output.empty()) {
            return;
        }

        ssize_t bytes_sent = send(client_fd,output.data(),output.size(),0);

        if (bytes_sent > 0) {
            output.erase(0, bytes_sent);
            if (output.empty()) {

                // No more data to send.
                // Stop watching this socket for EPOLLOUT.

                epoll_event client_event{};

                client_event.events = EPOLLIN;
                client_event.data.fd = client_fd;

                if (epoll_ctl(epoll_fd,EPOLL_CTL_MOD,client_fd,&client_event) < 0) {
                    std::cerr << "Failed to disable EPOLLOUT"<< std::endl;
                }
            }
        }
        else if (bytes_sent < 0) {

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }

            std::cerr << "send failed: "<< errno<< " - "<< strerror(errno)<< std::endl;
        }
    }
    void Server::enableWrite(int epoll_fd, int client_fd)
    {
        epoll_event client_event{};

        client_event.events = EPOLLIN | EPOLLOUT;
        client_event.data.fd = client_fd;

        if (epoll_ctl(epoll_fd,EPOLL_CTL_MOD,client_fd,&client_event) < 0) {
            std::cerr << "Failed to enable EPOLLOUT for client"<< std::endl;
        }
    }
    void Server::enqueueCommand(int client_fd,const std::string& command)
    {
        bool should_process = false;

        {
            std::lock_guard<std::mutex> lock(responseMutex_);

            auto it = clients_.find(client_fd);

            if (it == clients_.end()) {
                return;
            }

            ClientState& client = it->second;

            constexpr std::size_t MAX_QUEUE_SIZE = 1000;

            if (client.command_queue.size() >= MAX_QUEUE_SIZE) {
                responseQueue_.push(
                    ClientResponse{
                        client_fd,
                        client.generation,
                        "ERR server busy\n"
                    }
                );

                uint64_t value = 1;
                write(response_event_fd_, &value, sizeof(value));
                return;
            }
            client.command_queue.push_back(command);

            if (!client.processing) {
                client.processing = true;
                should_process = true;
            }
        }

        if (should_process) {
            processNextCommand(client_fd);
        }
    }
    void Server::processNextCommand(int client_fd)
    {
        threadPool_.submit([this, client_fd]() {

            while (true) {

                std::string command;
                uint64_t generation;

                {
                    std::lock_guard<std::mutex> lock(responseMutex_);

                    auto it = clients_.find(client_fd);

                    if (it == clients_.end()) {
                        return;
                    }

                    ClientState& client = it->second;

                    if (client.command_queue.empty()) {
                        client.processing = false;
                        return;
                    }

                    generation = client.generation;

                    command = std::move(client.command_queue.front());
                    client.command_queue.pop_front();
                }

                ClientResponse result = processCommand(client_fd, command);

                result.generation = generation;

                {
                    std::lock_guard<std::mutex> lock(responseMutex_);
                    responseQueue_.push(std::move(result));
                }

                uint64_t value = 1;
                if (write(response_event_fd_, &value, sizeof(value)) < 0) {
                    std::cerr << "eventfd write failed: "
                              << strerror(errno) << std::endl;
                }
            }
        });
    }
    bool Server::replicateWithReconnect(
        const std::string& command
    )
    {
        std::lock_guard<std::mutex> lock(
            replicationMutex_
        );

        if (!replication_enabled_) {
            return true;
        }

        forgedb::replication::ReplicationEntry
            new_entry;

        try {

            new_entry =
                replication_log_.append(
                    command
                );
        }
        catch (
            const std::exception& e
        ) {

            std::cerr
                << "Failed to append "
                << "replication log: "
                << e.what()
                << "\n";

            return false;
        }

        std::cout
            << "Created replication entry "
            << new_entry.sequence
            << "\n";

        std::vector<
            forgedb::replication::ReplicationEntry
        > entries;

        try {

            entries =
                replication_log_.load();
        }
        catch (
            const std::exception& e
        ) {

            std::cerr
                << "Failed to load "
                << "replication log: "
                << e.what()
                << "\n";

            return false;
        }

        for (
            const auto& entry :
            entries
        ) {

            /*
             * Entries below this cursor were
             * already acknowledged during this
             * leader process lifetime.
             */
            if (
                entry.sequence <
                next_replication_to_send_
            ) {
                continue;
            }

            auto sendEntry =
                [this, &entry]()
                    -> forgedb::replication::
                        ReplicationStatus {

                return replica_client_
                    .replicateEntry(
                        entry
                    );
            };

            auto status =
                sendEntry();

            /*
             * Transport failure:
             * reconnect once and retry the
             * same exact sequence number.
             */
            if (
                status ==
                forgedb::replication::
                    ReplicationStatus::
                        TRANSPORT_ERROR
            ) {

                std::cerr
                    << "Replica disconnected. "
                    << "Attempting reconnect...\n";

                if (
                    !replica_client_.connectTo(
                        replica_host_,
                        replica_port_
                    )
                ) {

                    std::cerr
                        << "Replica reconnect failed\n";

                    return false;
                }

                std::cout
                    << "Replica reconnected\n";

                status =
                    sendEntry();
            }

            if (
                status ==
                forgedb::replication::
                    ReplicationStatus::
                        TRANSPORT_ERROR
            ) {

                std::cerr
                    << "Replication transport "
                    << "failed for entry "
                    << entry.sequence
                    << "\n";

                return false;
            }

            if (
                status ==
                forgedb::replication::
                    ReplicationStatus::
                        REJECTED
            ) {

                std::cerr
                    << "Follower rejected "
                    << "replication entry "
                    << entry.sequence
                    << "\n";

                return false;
            }

            std::cout
                << "Replication entry "
                << entry.sequence
                << " acknowledged\n";

            next_replication_to_send_ =
                entry.sequence + 1;
        }

        return true;
    }

    bool Server::loadReplicationState()
    {
        constexpr const char* STATE_FILE =
            "replication.meta";

        if (
            !std::filesystem::exists(
                STATE_FILE
            )
        ) {

            last_applied_replication_sequence_ = 0;

            return true;
        }

        std::ifstream file(
            STATE_FILE
        );

        if (!file.is_open()) {

            std::cerr
                << "Failed to open replication state\n";

            return false;
        }

        uint64_t sequence = 0;

        file >> sequence;

        if (file.fail()) {

            std::cerr
                << "Invalid replication state\n";

            return false;
        }

        last_applied_replication_sequence_ =
            sequence;

        std::cout
            << "Loaded replication sequence "
            << sequence
            << "\n";

        return true;
    }
    bool Server::persistReplicationState(
        uint64_t sequence
    )
    {
        constexpr const char* STATE_FILE =
            "replication.meta";

        constexpr const char* TEMP_FILE =
            "replication.meta.tmp";

        std::string data =
            std::to_string(sequence) +
            "\n";

        int fd = open(
            TEMP_FILE,
            O_WRONLY |
            O_CREAT |
            O_TRUNC,
            0644
        );

        if (fd < 0) {

            std::cerr
                << "Failed to open replication "
                << "state temp file\n";

            return false;
        }

        std::size_t total_written = 0;

        while (
            total_written <
            data.size()
        ) {

            ssize_t written = write(
                fd,
                data.data() +
                    total_written,
                data.size() -
                    total_written
            );

            if (written < 0) {

                if (errno == EINTR) {
                    continue;
                }

                std::cerr
                    << "Failed to write "
                    << "replication state\n";

                close(fd);
                unlink(TEMP_FILE);

                return false;
            }

            if (written == 0) {

                std::cerr
                    << "Replication state write "
                    << "made no progress\n";

                close(fd);
                unlink(TEMP_FILE);

                return false;
            }

            total_written +=
                static_cast<std::size_t>(
                    written
                );
        }

        while (fsync(fd) < 0) {

            if (errno == EINTR) {
                continue;
            }

            std::cerr
                << "Failed to sync "
                << "replication state\n";

            close(fd);
            unlink(TEMP_FILE);

            return false;
        }

        close(fd);

        if (
            std::rename(
                TEMP_FILE,
                STATE_FILE
            ) != 0
        ) {

            std::cerr
                << "Failed to publish "
                << "replication state\n";

            unlink(TEMP_FILE);

            return false;
        }

        int directory_fd = open(
            ".",
            O_RDONLY |
            O_DIRECTORY
        );

        if (directory_fd < 0) {

            std::cerr
                << "Failed to open database "
                << "directory\n";

            return false;
        }

        while (
            fsync(directory_fd) < 0
        ) {

            if (errno == EINTR) {
                continue;
            }

            std::cerr
                << "Failed to sync database "
                << "directory\n";

            close(directory_fd);

            return false;
        }

        close(directory_fd);

        return true;
    }

    bool Server::replayReplicationLog()
    {
        std::lock_guard<std::mutex> lock(
            replicationMutex_
        );

        if (!replication_enabled_) {
            return true;
        }

        std::vector<
            forgedb::replication::ReplicationEntry
        > entries;

        try {
            entries =
                replication_log_.load();
        }
        catch (const std::exception& e) {
            std::cerr
                << "Failed to load replication log: "
                << e.what()
                << "\n";

            return false;
        }

        for (const auto& entry : entries) {

            if (
                entry.sequence <
                next_replication_to_send_
            ) {
                continue;
            }

            auto status =
                replica_client_.replicateEntry(
                    entry
                );

            if (
                status ==
                forgedb::replication::
                    ReplicationStatus::
                        TRANSPORT_ERROR
            ) {
                std::cerr
                    << "Replication transport failed "
                    << "during startup catch-up "
                    << "for entry "
                    << entry.sequence
                    << "\n";

                return false;
            }

            if (
                status ==
                forgedb::replication::
                    ReplicationStatus::
                        REJECTED
            ) {
                std::cerr
                    << "Follower rejected entry "
                    << entry.sequence
                    << " during startup catch-up\n";

                return false;
            }

            std::cout
                << "Replication entry "
                << entry.sequence
                << " acknowledged\n";

            next_replication_to_send_ =
                entry.sequence + 1;
        }

        return true;
    }

    ClientResponse
    Server::processRaftAppendRequest(
        int client_fd,
        const std::string& command
    )
    {
        auto request =
            forgedb::raft::
                decodeAppendEntries(
                    command
                );

        if (!request.has_value()) {

            return ClientResponse{
                client_fd,
                0,
                "ERR invalid Raft AppendEntries request\n"
            };
        }
        std::lock_guard<std::mutex> lock(
            raft_append_mutex_
        );
        try {

            /*
             * First deal with the term.
             *
             * This is done BEFORE checking
             * log consistency because a
             * valid leader in our term still
             * causes a candidate to step down.
             */
            if (
                !raft_state_.observeLeader(
                    request->term
                )
            ) {

                return ClientResponse{
                    client_fd,
                    0,
                    forgedb::raft::
                        encodeAppendEntriesResponse({
                            raft_state_.currentTerm(),
                            false
                        })
                };
            }

            /*
             * The sender is a legitimate leader for
             * our current term or a newer term.
             *
             * Reset BEFORE checking prevLogIndex /
             * prevLogTerm.
             *
             * Even if the log check fails, we still
             * heard from the current leader and must
             * not start an election.
             */
            election_timer_.reset();

            std::cout
                << "Election timer reset by leader "
                << request->leader_id
                << " in term "
                << request->term
                << "\n";

            /*
             * Raft log consistency check:
             *
             * Does our entry at prev_log_index
             * have prev_log_term?
             */
            auto local_term =
                raft_log_.termAt(
                    request->prev_log_index
                );

            if (
                !local_term.has_value() ||
                *local_term !=
                    request->prev_log_term
            ) {

                return ClientResponse{
                    client_fd,
                    0,
                    forgedb::raft::
                        encodeAppendEntriesResponse({
                            raft_state_.currentTerm(),
                            false
                        })
                };
            }
            /*
             * Incoming entries must immediately
             * follow prevLogIndex.
             */
            if (
                !request->entries.empty()
            ) {
                if (
                    request->prev_log_index ==
                    std::numeric_limits<uint64_t>::max() ||
                    request->entries.front().index !=
                        request->prev_log_index + 1
                ) {
                    return ClientResponse{
                        client_fd,
                        0,
                        forgedb::raft::
                            encodeAppendEntriesResponse({
                                raft_state_.currentTerm(),
                                false
                            })
                    };
                }
            }

            /*
             * An incoming conflicting entry must
             * never overwrite a committed entry.
             */
            const uint64_t committed =
                raft_commit_state_.commitIndex();

            for (
                const auto& incoming :
                request->entries
            ) {
                if (
                    incoming.index > committed
                ) {
                    break;
                }

                auto existing =
                    raft_log_.entryAt(
                        incoming.index
                    );

                if (
                    !existing.has_value() ||
                    existing->term != incoming.term ||
                    existing->command != incoming.command
                ) {
                    return ClientResponse{
                        client_fd,
                        0,
                        forgedb::raft::
                            encodeAppendEntriesResponse({
                                raft_state_.currentTerm(),
                                false
                            })
                    };
                }
            }

            /*
             * The prefix matched.
             *
             * The follower can now reconcile
             * any entries supplied by the leader.
             *
             * Empty entries simply mean that
             * this RPC was a heartbeat.
             */
            raft_log_.appendFromLeader(
                request->entries
            );
            /*
             * The last index verified by this
             * successful AppendEntries request.
             *
             * If entries is empty, the matching
             * prevLogIndex is the verified boundary.
             */
            uint64_t verified_index =
                request->prev_log_index;

            if (
                !request->entries.empty()
            ) {
                verified_index =
                    request->entries.back().index;
            }

            /*
             * Never commit an unverified divergent
             * suffix merely because it is present
             * in the local log.
             */
            const uint64_t safe_leader_commit =
                std::min(
                    request->leader_commit,
                    verified_index
                );

            const uint64_t local_last_index =
                raft_log_.lastLogInfo().first;

            const uint64_t old_commit =
                raft_commit_state_.commitIndex();

            const uint64_t new_commit =
                raft_commit_state_.advanceFollowerCommit(
                    safe_leader_commit,
                    local_last_index
                );

            if (
                new_commit > old_commit
            ) {
                std::cout
                    << node_id_
                    << " follower commitIndex advanced "
                    << old_commit
                    << " -> "
                    << new_commit
                    << "\n";
            }
            /*
             * Apply all committed entries not yet
             * reflected in the KeyValueStore.
             *
             * The application mutex guarantees
             * that parallel calls remain ordered.
             *
             * On failure, the node terminates.
             */
            applyCommittedOrTerminate();

            if (
                !request->entries.empty()
            ) {
                std::cout
                    << "Accepted "
                    << request->entries.size()
                    << " Raft log entr"
                    << (
                        request->entries.size() == 1
                            ? "y"
                            : "ies"
                    )
                    << " from leader "
                    << request->leader_id
                    << "\n";
            }

            /*
             * leader_commit is intentionally
             * not processed yet.
             *
             * At this stage we persist log
             * entries but do NOT apply them
             * to KeyValueStore.
             */
            return ClientResponse{
                client_fd,
                0,
                forgedb::raft::
                    encodeAppendEntriesResponse({
                        raft_state_.currentTerm(),
                        true
                    })
            };
        }
        catch (
            const std::exception& e
        ) {

            std::cerr
                << "AppendEntries processing failed: "
                << e.what()
                << "\n";

            return ClientResponse{
                client_fd,
                0,
                "ERR Raft persistence failure\n"
            };
        }
    }
}
