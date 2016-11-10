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
#include <commitmanager/SnapshotDescriptor.hpp>
#include <commitmanager/HashRing.hpp>
#include <commitmanager/Descriptor.hpp>

#include <crossbow/byte_buffer.hpp>
#include <crossbow/logger.hpp>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>

namespace tell {
namespace commitmanager {

std::unique_ptr<SnapshotDescriptor> SnapshotDescriptor::create(uint64_t lowestActiveVersion,
        const Descriptor& descriptor) {
    auto descLen = descriptorLength(descriptor.baseVersion(), descriptor.lastVersion());

    std::unique_ptr<SnapshotDescriptor> snapshot(new (descLen) SnapshotDescriptor(lowestActiveVersion,
            descriptor.baseVersion(), descriptor.lastVersion()));
    if (snapshot) {
        crossbow::buffer_writer writer(snapshot->data(), descLen);
        descriptor.serialize(writer);
    }
    return snapshot;
}

std::unique_ptr<SnapshotDescriptor> SnapshotDescriptor::create(uint64_t lowestActiveVersion, uint64_t baseVersion,
        uint64_t version, const char* descriptor) {
    auto descLen = descriptorLength(baseVersion, version);

    std::unique_ptr<SnapshotDescriptor> snapshot(new (descLen) SnapshotDescriptor(lowestActiveVersion, baseVersion,
            version));
    if (snapshot) {
        memcpy(snapshot->data(), descriptor, descLen);
    }
    return snapshot;
}

void* SnapshotDescriptor::operator new(size_t size, size_t descLen) {
    LOG_ASSERT(size == sizeof(SnapshotDescriptor), "Requested size does not match SnapshotDescriptor size");
    return ::malloc(size + descLen);
}

void SnapshotDescriptor::operator delete(void* ptr) {
    ::free(ptr);
}

std::unique_ptr<SnapshotDescriptor> SnapshotDescriptor::deserialize(crossbow::buffer_reader& reader) {
    if (!reader.canRead(3 * sizeof(uint64_t))) {
        throw std::length_error("Input buffer too small for snapshot header");
    }
    auto lowestActiveVersion = reader.read<uint64_t>();
    auto baseVersion = reader.read<uint64_t>();
    auto version = reader.read<uint64_t>();

    auto descLen = descriptorLength(baseVersion, version);
    if (!reader.canRead(descLen)) {
        throw std::length_error("Input buffer too small for descriptor");
    }
    auto descriptor = reader.read(descLen);

    std::unique_ptr<SnapshotDescriptor> snapshot(new (descLen) SnapshotDescriptor(lowestActiveVersion, baseVersion, version));
    if (snapshot) {
        memcpy(snapshot->data(), descriptor, descLen);
    }

    // snapshot->directoryVersion = reader.read<uint64_t>();
    // snapshot->numPeers = 0; // will be set later when parsing the peers string
    // uint32_t peersSize = reader.read<uint32_t>();
    // snapshot->peers = crossbow::string(reader.read(peersSize), peersSize);
    // snapshot->nodeRing = std::move(HashRing::deserialize(reader));

    return snapshot;
}

void SnapshotDescriptor::serialize(crossbow::buffer_writer& writer) const {
    auto descLen = descriptorLength(mBaseVersion, mVersion);
    if (!writer.canWrite(3 * sizeof(uint64_t) + descLen)) {
        throw std::length_error("Output buffer too small for snapshot");
    }
    writer.write<uint64_t>(mLowestActiveVersion);
    writer.write<uint64_t>(mBaseVersion);
    writer.write<uint64_t>(mVersion);
    writer.write(data(), descLen);
}

std::ostream& operator<<(std::ostream& out, const SnapshotDescriptor& rhs) {
    out << "SnapshotDescriptor ["
           "Lowest Active Version = " << rhs.mLowestActiveVersion << ", "
           "Base Version = " << rhs.mBaseVersion << ", "
           "Version = " << rhs.mVersion << ", "
           "Descriptor = [";

    auto first = true;
    for (auto i = rhs.mBaseVersion + 1u; i <= rhs.mVersion; ++i) {
        if (i % 8u == 1u || first) {
            out << (first ? "[" : " [") << i << "] ";
            first = false;
        }
        out << (rhs.inReadSet(i) ? "x" : "-");
    }
    out << "]]";

    return out;
}

} // namespace commitmanager
} // namespace tell
