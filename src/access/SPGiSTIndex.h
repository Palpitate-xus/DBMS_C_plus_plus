#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace dbms {

struct SPGiSTPointEntry {
    double x = 0;
    double y = 0;
    int64_t rid = 0;
};

// Simple quadtree-based SP-GiST index for POINT type
// Each node covers a rectangular region and splits into 4 quadrants
struct SPGiSTNode {
    double minX = 0, minY = 0, maxX = 0, maxY = 0;
    std::vector<SPGiSTPointEntry> points;
    std::unique_ptr<SPGiSTNode> children[4]; // NW, NE, SW, SE
    double unsplittableX = 0;
    double unsplittableY = 0;
    bool hasUnsplittableCoordinate = false;
    bool cannotSplit = false;
    static constexpr size_t MAX_LEAF_POINTS = 16;

    bool isLeaf() const {
        return !children[0] && !children[1] && !children[2] && !children[3];
    }
    int quadrant(double x, double y) const {
        double midX = (minX + maxX) * 0.5;
        double midY = (minY + maxY) * 0.5;
        if (x <= midX && y >= midY) return 0; // NW
        if (x >= midX && y >= midY) return 1; // NE
        if (x <= midX && y <= midY) return 2; // SW
        return 3; // SE
    }
    void split();
};

class SPGiSTIndex {
public:
    explicit SPGiSTIndex(double worldMinX, double worldMinY,
                         double worldMaxX, double worldMaxY);

    void insert(double x, double y, int64_t rid);
    void remove(double x, double y, int64_t rid);

    // Search operators for POINT
    std::vector<int64_t> searchEquals(double x, double y) const;
    std::vector<int64_t> searchLeftOf(double x) const;     // point << (x, y)
    std::vector<int64_t> searchRightOf(double x) const;    // point >> (x, y)
    std::vector<int64_t> searchBelow(double y) const;      // point <^ (x, y)
    std::vector<int64_t> searchAbove(double y) const;      // point >^ (x, y)
    std::vector<int64_t> searchWithin(double cx, double cy, double radius) const;

    void clear();
    size_t size() const { return size_; }

private:
    SPGiSTNode root_;
    size_t size_ = 0;

    void insertRecursive(SPGiSTNode* node, double x, double y, int64_t rid);
    bool removeRecursive(SPGiSTNode* node, double x, double y, int64_t rid);
    void searchEqualsRecursive(const SPGiSTNode* node, double x, double y,
                               std::vector<int64_t>& out) const;
    void searchRegionRecursive(const SPGiSTNode* node,
                               double qminX, double qminY,
                               double qmaxX, double qmaxY,
                               std::vector<int64_t>& out) const;
    void searchWithinRecursive(const SPGiSTNode* node,
                               double cx, double cy, double radius,
                               std::vector<int64_t>& out) const;
};

} // namespace dbms
