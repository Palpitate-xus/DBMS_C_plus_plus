#include "SPGiSTIndex.h"
#include <cmath>

namespace dbms {

void SPGiSTNode::split() {
    if (!isLeaf() || points.size() <= MAX_LEAF_POINTS) return;
    double midX = (minX + maxX) * 0.5;
    double midY = (minY + maxY) * 0.5;
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
    root_.minX = worldMinX;
    root_.minY = worldMinY;
    root_.maxX = worldMaxX;
    root_.maxY = worldMaxY;
}

void SPGiSTIndex::insert(double x, double y, int64_t rid) {
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
    searchRegionRecursive(&root_, root_.minX, root_.minY, x, root_.maxY, result);
    return result;
}

std::vector<int64_t> SPGiSTIndex::searchRightOf(double x) const {
    std::vector<int64_t> result;
    searchRegionRecursive(&root_, x, root_.minY, root_.maxX, root_.maxY, result);
    return result;
}

std::vector<int64_t> SPGiSTIndex::searchBelow(double y) const {
    std::vector<int64_t> result;
    searchRegionRecursive(&root_, root_.minX, root_.minY, root_.maxX, y, result);
    return result;
}

std::vector<int64_t> SPGiSTIndex::searchAbove(double y) const {
    std::vector<int64_t> result;
    searchRegionRecursive(&root_, root_.minX, y, root_.maxX, root_.maxY, result);
    return result;
}

std::vector<int64_t> SPGiSTIndex::searchWithin(double cx, double cy, double radius) const {
    std::vector<int64_t> result;
    if (radius < 0.0 || std::isnan(radius)) return result;
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
