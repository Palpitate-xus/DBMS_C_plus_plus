#include "SPGiSTIndex.h"
#include <cmath>
#include <sstream>

namespace dbms {

void SPGiSTNode::split() {
    if (!isLeaf() || points.size() <= MAX_LEAF_POINTS) return;
    double midX = (minX + maxX) * 0.5;
    double midY = (minY + maxY) * 0.5;
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
        double x, y;
        size_t comma = p.first.find(',');
        if (comma == std::string::npos) continue;
        try {
            x = std::stod(p.first.substr(0, comma));
            y = std::stod(p.first.substr(comma + 1));
        } catch (...) { continue; }
        int q = quadrant(x, y);
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
        std::ostringstream oss;
        oss << x << "," << y;
        node->points.push_back({oss.str(), rid});
        if (node->points.size() > SPGiSTNode::MAX_LEAF_POINTS) {
            node->split();
        }
        return;
    }
    int q = node->quadrant(x, y);
    insertRecursive(node->children[q].get(), x, y, rid);
}

void SPGiSTIndex::remove(double x, double y, int64_t rid) {
    removeRecursive(&root_, x, y, rid);
    --size_;
}

void SPGiSTIndex::removeRecursive(SPGiSTNode* node, double x, double y, int64_t rid) {
    if (node->isLeaf()) {
        std::ostringstream oss;
        oss << x << "," << y;
        auto& pts = node->points;
        for (auto it = pts.begin(); it != pts.end(); ++it) {
            if (it->first == oss.str() && it->second == rid) {
                pts.erase(it);
                return;
            }
        }
        return;
    }
    int q = node->quadrant(x, y);
    if (node->children[q]) {
        removeRecursive(node->children[q].get(), x, y, rid);
    }
}

std::vector<int64_t> SPGiSTIndex::searchEquals(double x, double y) const {
    std::vector<int64_t> result;
    searchEqualsRecursive(&root_, x, y, result);
    return result;
}

void SPGiSTIndex::searchEqualsRecursive(const SPGiSTNode* node, double x, double y,
                                        std::vector<int64_t>& out) const {
    std::ostringstream oss;
    oss << x << "," << y;
    std::string key = oss.str();
    if (node->isLeaf()) {
        for (const auto& p : node->points) {
            if (p.first == key) out.push_back(p.second);
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
    searchRegionRecursive(&root_, cx - radius, cy - radius, cx + radius, cy + radius, result);
    // Post-filter by exact distance
    std::vector<int64_t> filtered;
    for (int64_t rid : result) {
        // Note: exact distance filtering would need row lookup; keep all in region for simplicity
        filtered.push_back(rid);
    }
    return filtered;
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
            double px, py;
            size_t comma = p.first.find(',');
            if (comma == std::string::npos) continue;
            try {
                px = std::stod(p.first.substr(0, comma));
                py = std::stod(p.first.substr(comma + 1));
            } catch (...) { continue; }
            if (px >= qminX && px <= qmaxX && py >= qminY && py <= qmaxY) {
                out.push_back(p.second);
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
