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
#include <commitmanager/ClientSocket.hpp>

#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>

int main(int argc, const char** argv) {
    crossbow::string commitManagerHost;
    bool help = false;
    crossbow::string logLevel("DEBUG");

    auto opts = crossbow::program_options::create_options(argv[0],
            crossbow::program_options::value<'h'>("help", &help),
            crossbow::program_options::value<'l'>("log-level", &logLevel),
            crossbow::program_options::value<'c'>("commit-manager", &commitManagerHost));

    try {
        crossbow::program_options::parse(opts, argc, argv);
    } catch (crossbow::program_options::argument_not_found e) {
        std::cerr << e.what() << std::endl << std::endl;
        crossbow::program_options::print_help(std::cout, opts);
        return 1;
    }

    if (help) {
        crossbow::program_options::print_help(std::cout, opts);
        return 0;
    }

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);

    crossbow::infinio::InfinibandLimits infinibandLimits;
    infinibandLimits.receiveBufferCount = 128;
    infinibandLimits.sendBufferCount = 128;
    infinibandLimits.bufferLength = 32 * 1024;
    infinibandLimits.sendQueueLength = 128;
    infinibandLimits.maxScatterGather = 1;

    crossbow::infinio::InfinibandService service(infinibandLimits);

    auto processor = service.createProcessor();
    tell::commitmanager::ClientSocket client(service.createSocket(*processor));
    client.connect(crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), commitManagerHost));

    processor->executeFiber([&client] (crossbow::infinio::Fiber& fiber) {
        LOG_INFO("Registering with the node directory");
        auto registerResponse = client.registerNode(fiber, "somenode:8080", "STORAGE");
        if (!registerResponse->waitForResult()) {
            auto& ec = registerResponse->error();
            LOG_INFO("Error while receiving cluster information [error = %1% %2%]", ec, ec.message());
            return;
        }
        auto clusterMeta = registerResponse->get();
        LOG_INFO("Received storage node info: %1%", clusterMeta->hosts);
        
        LOG_INFO("Responsible for ranges:");
        for (auto const& range : clusterMeta->ranges) {
            LOG_INFO("\t[%1%, %2%] owned by %3%", (uint64_t) range.start, (uint64_t) range.end, range.owner);
        }

        LOG_INFO("Unregistering with the node directory");
        auto unregisterResponse = client.unregisterNode(fiber, "somenode:8080");
        if (!unregisterResponse->waitForResult()) {
            auto& ec = unregisterResponse->error();
            LOG_INFO("Error while reading cluster information [error = %1% %2%]", ec, ec.message());
            return;
        }
        auto unregisterSucceeded = unregisterResponse->get();
        LOG_INFO("Unregistering succeeded? %1%", unregisterSucceeded);

        LOG_INFO("Re-registering with the node directory");
        auto reRegisterResponse = client.registerNode(fiber, "somenode:8080", "STORAGE");
        if (!reRegisterResponse->waitForResult()) {
            auto& ec = reRegisterResponse->error();
            LOG_INFO("Error while receiving cluster information [error = %1% %2%]", ec, ec.message());
            return;
        }
        auto newClusterMeta = reRegisterResponse->get();
        LOG_INFO("Received storage node info: %1%", newClusterMeta->hosts);

        LOG_INFO("Starting transaction");
        auto startResponse = client.startTransaction(fiber, false);
        if (!startResponse->waitForResult()) {
            auto& ec = startResponse->error();
            LOG_INFO("Error while starting transaction [error = %1% %2%]", ec, ec.message());
            return;
        }
        auto snapshot = startResponse->get();
        LOG_INFO("Started transaction [snapshot = %1%]", *snapshot);


        LOG_INFO("Committing transaction");
        auto commitResponse = client.commitTransaction(fiber, snapshot->version());
        if (!commitResponse->waitForResult()) {
            auto& ec = commitResponse->error();
            LOG_INFO("Error while committing transaction [error = %1% %2%]", ec, ec.message());
            return;
        }
        auto succeeded = commitResponse->get();
        LOG_INFO("Committed transaction [succeeded = %1%]", succeeded);
    });

    service.run();

    return 0;
}
