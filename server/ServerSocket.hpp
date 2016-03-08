/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once

#include "ServerConfig.hpp"

#include <commitmanager/CommitManager.hpp>
#include <server/DirectoryEntry.hpp>

#include <crossbow/byte_buffer.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/infinio/InfinibandSocket.hpp>
#include <crossbow/infinio/RpcServer.hpp>

#include <memory>

namespace tell {
namespace commitmanager {

class ServerManager;

/**
 * @brief Handles communication with one CommitManager client
 *
 * Listens for incoming RPC requests, performs the desired action and sends the response back to the client.
 */
class ServerSocket : public crossbow::infinio::RpcServerSocket<ServerManager, ServerSocket> {
    using Base = crossbow::infinio::RpcServerSocket<ServerManager, ServerSocket>;

public:
    ServerSocket(ServerManager& manager, crossbow::infinio::InfinibandProcessor& processor,
            crossbow::infinio::InfinibandSocket socket, size_t maxBatchSize)
            : Base(manager, processor, std::move(socket), crossbow::string(), maxBatchSize) {
    }

private:
    friend Base;
    friend class ServerManager;

    void onRequest(crossbow::infinio::MessageId messageId, uint32_t messageType, crossbow::buffer_reader& message);
};

class ServerManager : public crossbow::infinio::RpcServerManager<ServerManager, ServerSocket> {
    using Base = crossbow::infinio::RpcServerManager<ServerManager, ServerSocket>;

public:
    ServerManager(crossbow::infinio::InfinibandService& service, const ServerConfig& config);

private:
    friend Base;
    friend class ServerSocket;

    ServerSocket* createConnection(crossbow::infinio::InfinibandSocket socket, const crossbow::string& data);

    void onMessage(ServerSocket* con, crossbow::infinio::MessageId messageId, uint32_t messageType,
            crossbow::buffer_reader& message);

    void handleStartTransaction(ServerSocket* con, crossbow::infinio::MessageId messageId,
            crossbow::buffer_reader& message);

    void handleCommitTransaction(ServerSocket* con, crossbow::infinio::MessageId messageId,
            crossbow::buffer_reader& message);

    void handleGetNodes(ServerSocket* con, crossbow::infinio::MessageId messageId,
                                 crossbow::buffer_reader& message);

    std::unique_ptr<crossbow::infinio::InfinibandProcessor> mProcessor;

    size_t mMaxBatchSize;

    CommitManager mCommitManager;

    std::vector<struct DirectoryEntry> mDirectory;
};

} // namespace commitmanager
} // namespace tell
