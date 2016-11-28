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

#include <commitmanager/HashRing.hpp>

#include <crossbow/non_copyable.hpp>

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <new>

namespace crossbow {
class buffer_reader;
class buffer_writer;
} // namespace crossbow

namespace tell {
namespace commitmanager {

class Descriptor;

/**
 * @brief Descriptor containing information about the versions a transaction is allowed to read
 */
class SnapshotDescriptor final : crossbow::non_copyable, crossbow::non_movable {
public: // Construction
    static std::unique_ptr<SnapshotDescriptor> create(uint64_t lowestActiveVersion, const Descriptor& descriptor);

    static std::unique_ptr<SnapshotDescriptor> create(uint64_t lowestActiveVersion, uint64_t baseVersion,
            uint64_t version, const char* descriptor);

    void* operator new(size_t size, size_t descLen);

    void* operator new[](size_t size) = delete;

    void operator delete(void* ptr);

    void operator delete[](void* ptr) = delete;

public: // Serialization
    static std::unique_ptr<SnapshotDescriptor> deserialize(crossbow::buffer_reader& reader);

    static size_t descriptorLength(uint64_t baseVersion, uint64_t lastVersion) {
        if (baseVersion >= lastVersion) {
            return 0x0u;
        }
        return (((lastVersion - 1) / BITS_PER_BLOCK) - (baseVersion / BITS_PER_BLOCK) + 1) * sizeof(BlockType);
    }

    void readNodeRing(crossbow::buffer_reader& reader);

    size_t serializedLength() const {
        return (3 * sizeof(uint64_t)) + descriptorLength(mBaseVersion, mVersion);
    }

    void serialize(crossbow::buffer_writer& writer) const;

public: // Version
    using BlockType = uint8_t;

    static constexpr size_t BITS_PER_BLOCK = sizeof(BlockType) * 8u;

    uint64_t lowestActiveVersion() const {
        return mLowestActiveVersion;
    }

    uint64_t baseVersion() const {
        return mBaseVersion;
    }

    uint64_t version() const {
        return mVersion;
    }

    const char* data() const {
        return reinterpret_cast<const char*>(this) + sizeof(SnapshotDescriptor);
    }

    bool inReadSet(uint64_t version) const {
        if (version <= mBaseVersion) {
            return true;
        }
        if (version > mVersion) {
            return false;
        }

        auto block = reinterpret_cast<const BlockType*>(data())[blockIndex(version)];
        auto mask = (0x1u << ((version - 1) % BITS_PER_BLOCK));
        return (block & mask) != 0x0u;
    }

    bool inReadSet(uint64_t validFrom, uint64_t validTo) const {
        return (inReadSet(validFrom) && !inReadSet(validTo));
    }

    std::unique_ptr<HashRing> nodeRing;
    size_t numPeers;
    uint64_t directoryVersion;
    crossbow::string peers;
    
private:
    friend std::ostream& operator<<(std::ostream& out, const SnapshotDescriptor& rhs);

    SnapshotDescriptor(uint64_t lowestActiveVersion, uint64_t baseVersion, uint64_t version)
            : mLowestActiveVersion(lowestActiveVersion),
              mBaseVersion(baseVersion),
              mVersion(version) {
    }

    size_t blockIndex(uint64_t version) const {
        return (((version - 1) / BITS_PER_BLOCK) - (mBaseVersion / BITS_PER_BLOCK));
    }

    char* data() {
        return const_cast<char*>(const_cast<const SnapshotDescriptor*>(this)->data());
    }

    uint64_t mLowestActiveVersion;
    uint64_t mBaseVersion;
    uint64_t mVersion;
};

std::ostream& operator<<(std::ostream& out, const SnapshotDescriptor& rhs);

} // namespace commitmanager
} // namespace tell
