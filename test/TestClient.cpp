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
#include <commitmanager/HashRing.hpp>

#include <crossbow/infinio/Endpoint.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>


using namespace tell::commitmanager;


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
    ClientSocket client(service.createSocket(*processor));
    client.connect(crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), commitManagerHost));

    // processor->executeFiber([&client] (crossbow::infinio::Fiber& fiber) {
    //     crossbow::string token = "somenode:8080";
    //     crossbow::string tag = "STORAGE";

    //     LOG_INFO("Starting transaction");
    //     auto txResp = client.startTransaction(fiber, false);
    //     if (!txResp->waitForResult()) {
    //         auto& ec = txResp->error();
    //         LOG_ERROR("Error while starting transaction [error = %1% %2%]", ec, ec.message());
    //         return;
    //     }
    //     auto snapshot = txResp->get();
    //     LOG_INFO("Started transaction [snapshot = %1%]", *snapshot);

    //     LOG_INFO("Registering with the node directory");
    //     auto registerResp = client.registerNode(fiber, *snapshot, token, tag);
    //     if (!registerResp->waitForResult()) {
    //         auto& ec = registerResp->error();
    //         LOG_ERROR("Error while receiving cluster information [error = %1% %2%]", ec, ec.message());
    //         return;
    //     }
    //     auto clusterMeta = registerResp->get();
        
    //     LOG_INFO("Responsible for ranges:");
    //     for (const auto& range : clusterMeta->ranges) {
    //         LOG_INFO("\t[%1%, %2%] owned by %3%", HashRing::writeHash(range.start), HashRing::writeHash(range.end), range.owner);
    //     }

    //     LOG_INFO("Committing transaction");
    //     auto commitResp = client.commitTransaction(fiber, snapshot->version());
    //     if (!commitResp->waitForResult()) {
    //         auto& ec = commitResp->error();
    //         LOG_ERROR("Error while committing transaction [error = %1% %2%]", ec, ec.message());
    //         return;
    //     }
    //     LOG_INFO("Committed transaction [succeeded = %1%]", commitResp->get());

    //     LOG_INFO("Starting transaction");
    //     txResp = client.startTransaction(fiber, false);
    //     if (!txResp->waitForResult()) {
    //         auto& ec = txResp->error();
    //         LOG_ERROR("Error while starting transaction [error = %1% %2%]", ec, ec.message());
    //         return;
    //     }
    //     snapshot = txResp->get();
    //     LOG_INFO("Started transaction [snapshot = %1%]", *snapshot);

    //     LOG_INFO("Unregistering with the node directory");
    //     auto unregisterResp = client.unregisterNode(fiber, *snapshot, token);
    //     if (!unregisterResp->waitForResult()) {
    //         auto& ec = unregisterResp->error();
    //         LOG_ERROR("Error while reading cluster information [error = %1% %2%]", ec, ec.message());
    //         return;
    //     }
    //     clusterMeta = unregisterResp->get();
        
    //     LOG_INFO("Giving up ranges:");
    //     for (const auto& range : clusterMeta->ranges) {
    //         LOG_INFO("\t[%1%, %2%] -> %3%", HashRing::writeHash(range.start), HashRing::writeHash(range.end), range.owner);
    //     }

    //     LOG_INFO("Committing transaction");
    //     commitResp = client.commitTransaction(fiber, snapshot->version());
    //     if (!commitResp->waitForResult()) {
    //         auto& ec = commitResp->error();
    //         LOG_ERROR("Error while committing transaction [error = %1% %2%]", ec, ec.message());
    //         return;
    //     }
    //     LOG_INFO("Committed transaction [succeeded = %1%]", commitResp->get());
    // });

    service.run();

    return 0;
}
