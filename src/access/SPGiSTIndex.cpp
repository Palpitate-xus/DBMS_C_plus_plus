#include "SPGiSTIndex.h"
#include <algorithm>
#include <cmath>
#include <limits>

namespace dbms {

namespace {

void collectPoints(const SPGiSTNode* node, std::vector<SPGiSTPointEntry>& out) {
    if (!node) return;
    if (node->isLeaf()) {
        out.insert(out.end(), node->points.begin(), node->points.end());
        return;
    }
    for (const auto& child : node->children) collectPoints(child.get(), out);
}

std::pair<double, double> expandInterval(double low, double high, double value) {
    if (value >= low && value <= high) return {low, high};

    long double width = static_cast<long double>(high) - low;
    if (!(width > 0.0L)) width = 1.0L;
    const long double lowest = std::numeric_limits<double>::lowest();
    const long double highest = std::numeric_limits<double>::max();
    if (value < low) {
        const long double grown = static_cast<long double>(high) - width * 2.0L;
        low = static_cast<double>(std::max(lowest,
            std::min(static_cast<long double>(value), grown)));
    } else {
        const long double grown = static_cast<long double>(low) + width * 2.0L;
        high = static_cast<double>(std::min(highest,
            std::max(static_cast<long double>(value), grown)));
    }
    return {low, high};
}

}  // namespace

void SPGiSTNode::split() {
    if (!isLeaf() || points.size() <= MAX_LEAF_POINTS) return;
    double midX = minX * 0.5 + maxX * 0.5;
    double midY = minY * 0.5 + maxY * 0.5;
    const bool xCanShrink = midX > minX && midX < maxX;
    const bool yCanShrink = midY > minY && midY < maxY;
    if (!xCanShrink && !yCanShrink) {
        cannotSplit = true;
        return;
    }

    // A quadtree cannot separate multiple entries at exactly the same
    // coordinate. Keeping them in one leaf avoids creating one new level for
    // every duplicate insert.
    bool identicalCoordinates = true;
    for (size_t i = 1; i < points.size(); ++i) {
        if (points[i].x != points[0].x || points[i].y != points[0].y) {
            identicalCoordinates = false;
            break;
        }
    }
    if (identicalCoordinates) {
        unsplittableX = points[0].x;
        unsplittableY = points[0].y;
        hasUnsplittableCoordinate = true;
        return;
    }
    hasUnsplittableCoordinate = false;

    children[0] = std::make_unique<SPGiSTNode>();
    children[0]->minX = minX; children[0]->minY = midY;
    children[0]->maxX = midX; children[0]->maxY = maxY;
    children[1] = std::make_unique<SPGiSTNode>();
    children[1]->minX = midX; children[1]->minY = midY;
    children[1]->maxX = maxX; children[1]->maxY = maxY;
    children[2] = std::make_unique<SPGiSTNode>();
    children[2]->minX = minX; children[2]->minY = minY;
    children[2]->maxX = midX; children[2]->maxY = midY;
    children[3] = std::make_unique<SPGiSTNode>();
    children[3]->minX = midX; children[3]->minY = minY;
    children[3]->maxX = maxX; children[3]->maxY = midY;
    for (const auto& p : points) {
        int q = quadrant(p.x, p.y);
        children[q]->points.push_back(p);
    }
    points.clear();
}

SPGiSTIndex::SPGiSTIndex(double worldMinX, double worldMinY,
                         double worldMaxX, double worldMaxY) {
    if (worldMinX > worldMaxX) std::swap(worldMinX, worldMaxX);
    if (worldMinY > worldMaxY) std::swap(worldMinY, worldMaxY);
    root_.minX = worldMinX;
    root_.minY = worldMinY;
    root_.maxX = worldMaxX;
    root_.maxY = worldMaxY;
}

void SPGiSTIndex::insert(double x, double y, int64_t rid) {
    if (!std::isfinite(x) || !std::isfinite(y)) return;
    if (x < root_.minX || x > root_.maxX ||
        y < root_.minY || y > root_.maxY) {
        // Node bounds are used for pruning.  Keeping an out-of-range point in
        // a boundary child would make range/radius scans skip a real match.
        // Expand geometrically and rebuild so every descendant bound remains
        // truthful.
        std::vector<SPGiSTPointEntry> existing;
        existing.reserve(size_);
        collectPoints(&root_, existing);
        const auto xBounds = expandInterval(root_.minX, root_.maxX, x);
        const auto yBounds = expandInterval(root_.minY, root_.maxY, y);
        root_.minX = xBounds.first;
        root_.maxX = xBounds.second;
        root_.minY = yBounds.first;
        root_.maxY = yBounds.second;
        root_.points.clear();
        for (auto& child : root_.children) child.reset();
        root_.hasUnsplittableCoordinate = false;
        root_.cannotSplit = false;
        for (const auto& point : existing)
            insertRecursive(&root_, point.x, point.y, point.rid);
    }
    insertRecursive(&root_, x, y, rid);
    ++size_;
}

void SPGiSTIndex::insertRecursive(SPGiSTNode* node, double x, double y, int64_t rid) {
    if (node->isLeaf()) {
        node->points.push_back({x, y, rid});
        if (node->points.size() > SPGiSTNode::MAX_LEAF_POINTS &&
            !node->cannotSplit &&
            (!node->hasUnsplittableCoordinate ||
             node->unsplittableX != x || node->unsplittableY != y)) {
            node->split();
        }
        return;
    }
    int q = node->quadrant(x, y);
    insertRecursive(node->children[q].get(), x, y, rid);
}

void SPGiSTIndex::remove(double x, double y, int64_t rid) {
    if (!std::isfinite(x) || !std::isfinite(y)) return;
    if (removeRecursive(&root_, x, y, rid)) {
        --size_;
    }
}

bool SPGiSTIndex::removeRecursive(SPGiSTNode* node, double x, double y, int64_t rid) {
    if (node->isLeaf()) {
        auto& pts = node->points;
        for (auto it = pts.begin(); it != pts.end(); ++it) {
            if (it->x == x && it->y == y && it->rid == rid) {
                pts.erase(it);
                return true;
            }
        }
        return false;
    }
    int q = node->quadrant(x, y);
    if (node->children[q]) {
        return removeRecursive(node->children[q].get(), x, y, rid);
    }
    return false;
}

std::vector<int64_t> SPGiSTIndex::searchEquals(double x, double y) const {
    std::vector<int64_t> result;
    if (!std::isfinite(x) || !std::isfinite(y)) return result;
    searchEqualsRecursive(&root_, x, y, result);
    return result;
}

void SPGiSTIndex::searchEqualsRecursive(const SPGiSTNode* node, double x, double y,
                                        std::vector<int64_t>& out) const {
    if (node->isLeaf()) {
        for (const auto& p : node->points) {
            if (p.x == x && p.y == y) out.push_back(p.rid);
        }
        return;
    }
    int q = node->quadrant(x, y);
    if (node->children[q]) {
        searchEqualsRecursive(node->children[q].get(), x, y, out);
    }
}

std::vector<int64_t> SPGiSTIndex::searchLeftOf(double x) const {
    std::vector<int64_t> result;
    if (!std::isfinite(x)) return result;
    const double strictMax = std::nextafter(x,
        -std::numeric_limits<double>::infinity());
    searchRegionRecursive(&root_, root_.minX, root_.minY,
                          strictMax, root_.maxY, result);
    return result;
}

std::vector<int64_t> SPGiSTIndex::searchRightOf(double x) const {
    std::vector<int64_t> result;
    if (!std::isfinite(x)) return result;
    const double strictMin = std::nextafter(x,
        std::numeric_limits<double>::infinity());
    searchRegionRecursive(&root_, strictMin, root_.minY,
                          root_.maxX, root_.maxY, result);
    return result;
}

std::vector<int64_t> SPGiSTIndex::searchBelow(double y) const {
    std::vector<int64_t> result;
    if (!std::isfinite(y)) return result;
    const double strictMax = std::nextafter(y,
        -std::numeric_limits<double>::infinity());
    searchRegionRecursive(&root_, root_.minX, root_.minY,
                          root_.maxX, strictMax, result);
    return result;
}

std::vector<int64_t> SPGiSTIndex::searchAbove(double y) const {
    std::vector<int64_t> result;
    if (!std::isfinite(y)) return result;
    const double strictMin = std::nextafter(y,
        std::numeric_limits<double>::infinity());
    searchRegionRecursive(&root_, root_.minX, strictMin,
                          root_.maxX, root_.maxY, result);
    return result;
}

std::vector<int64_t> SPGiSTIndex::searchWithin(double cx, double cy, double radius) const {
    std::vector<int64_t> result;
    if (!std::isfinite(cx) || !std::isfinite(cy) ||
        !std::isfinite(radius) || radius < 0.0) return result;
    searchWithinRecursive(&root_, cx, cy, radius, result);
    return result;
}

void SPGiSTIndex::searchWithinRecursive(const SPGiSTNode* node,
                                        double cx, double cy, double radius,
                                        std::vector<int64_t>& out) const {
    if (!node) return;

    const double dx = cx < node->minX ? node->minX - cx
                      : cx > node->maxX ? cx - node->maxX : 0.0;
    const double dy = cy < node->minY ? node->minY - cy
                      : cy > node->maxY ? cy - node->maxY : 0.0;
    if (std::hypot(dx, dy) > radius) return;

    if (node->isLeaf()) {
        for (const auto& point : node->points) {
            if (std::hypot(point.x - cx, point.y - cy) <= radius) {
                out.push_back(point.rid);
            }
        }
        return;
    }

    for (const auto& child : node->children) {
        searchWithinRecursive(child.get(), cx, cy, radius, out);
    }
}

void SPGiSTIndex::searchRegionRecursive(const SPGiSTNode* node,
                                        double qminX, double qminY,
                                        double qmaxX, double qmaxY,
                                        std::vector<int64_t>& out) const {
    if (!node) return;
    // No overlap
    if (node->maxX < qminX || node->minX > qmaxX || node->maxY < qminY || node->minY > qmaxY) {
        return;
    }
    if (node->isLeaf()) {
        for (const auto& p : node->points) {
            if (p.x >= qminX && p.x <= qmaxX &&
                p.y >= qminY && p.y <= qmaxY) {
                out.push_back(p.rid);
            }
        }
        return;
    }
    for (int i = 0; i < 4; ++i) {
        if (node->children[i]) {
            searchRegionRecursive(node->children[i].get(), qminX, qminY, qmaxX, qmaxY, out);
        }
    }
}

void SPGiSTIndex::clear() {
    for (int i = 0; i < 4; ++i) root_.children[i].reset();
    root_.points.clear();
    size_ = 0;
}

} // namespace dbms
