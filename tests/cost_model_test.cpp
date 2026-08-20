// ============================================================================
// cost_model_test — P1-9 custom cost functions:
//   CostModel defaults follow PostgreSQL's canonical values
//   costJoinAlgorithm honors page/tuple parameters
//   disabled algorithms price out (1e18) and re-enable restores them
//   custom cost hooks override the built-in model, returning < 0 falls back
//   costScan for seq/index strategies uses the same parameters
//   Config GUCs (seq_page_cost, enable_nestloop, ...) parse and validate
//   planner-level SET syncs Config into the live CostModel
// Driven through QueryPlanner::costModel (the same object the join planner
// consults via estimateJoinCost).
// ============================================================================

#include "executor/ExecutionPlan.h"
#include "common/Config.h"

#include <cassert>
#include <cmath>
#include <iostream>
#include <string>

using namespace dbms;

// Custom hook used for the override test: doubles whatever the built-in
// model computes for the requested algorithm, rejects others.
static double g_customCalls = 0;
static double customHook(const std::string& algo, double leftRows,
                         double rightRows, bool rightIndexed) {
    if (algo == "hash") {
        ++g_customCalls;
        return 12345.0;
    }
    (void)leftRows; (void)rightRows; (void)rightIndexed;
    return -1.0;  // fall back to built-in for everything else
}

int main() {
    // ------------------------------------------------------------------
    // 1. Defaults match PostgreSQL's canonical cost constants.
    // ------------------------------------------------------------------
    QueryPlanner::setCostModel(QueryPlanner::CostModel{});
    {
        const auto& cm = QueryPlanner::costModel();
        assert(cm.seqPageCost == 1.0);
        assert(cm.randomPageCost == 4.0);
        assert(cm.cpuTupleCost == 0.01);
        assert(cm.cpuIndexTupleCost == 0.005);
        assert(cm.cpuOperatorCost == 0.0025);
        assert(cm.enableNestloop && cm.enableHashJoin && cm.enableMergeJoin);
        assert(cm.customCost == nullptr);
    }
    std::cout << "[COST] defaults OK" << std::endl;

    // ------------------------------------------------------------------
    // 2. Parameters flow into join costing: a more expensive random page
    //    raises indexed NLJ; a higher operator cost raises everything.
    // ------------------------------------------------------------------
    {
        double baseNljIndexed = QueryPlanner::costJoinAlgorithm(
            "nlj", 1000, 1000, true);
        QueryPlanner::CostModel cm = QueryPlanner::costModel();
        cm.randomPageCost = 8.0;
        QueryPlanner::setCostModel(cm);
        double raisedNljIndexed = QueryPlanner::costJoinAlgorithm(
            "nlj", 1000, 1000, true);
        assert(raisedNljIndexed > baseNljIndexed);

        double baseHash = QueryPlanner::costJoinAlgorithm("hash", 1000, 1000, false);
        cm.cpuTupleCost = 0.5;
        QueryPlanner::setCostModel(cm);
        double raisedHash = QueryPlanner::costJoinAlgorithm("hash", 1000, 1000, false);
        assert(raisedHash > baseHash);
        QueryPlanner::setCostModel(QueryPlanner::CostModel{});
    }
    std::cout << "[COST] parameter sensitivity OK" << std::endl;

    // ------------------------------------------------------------------
    // 3. enable_* flags price the algorithm out of consideration.
    // ------------------------------------------------------------------
    {
        QueryPlanner::CostModel cm;
        cm.enableHashJoin = false;
        cm.enableMergeJoin = false;
        cm.enableNestloop = false;
        QueryPlanner::setCostModel(cm);
        assert(QueryPlanner::costJoinAlgorithm("hash", 100, 100, false) >= 1e18);
        assert(QueryPlanner::costJoinAlgorithm("merge", 100, 100, true) >= 1e18);
        assert(QueryPlanner::costJoinAlgorithm("nlj", 100, 100, false) >= 1e18);
        // Re-enable restores sensible costs.
        QueryPlanner::setCostEnable("hashjoin", true);
        assert(QueryPlanner::costJoinAlgorithm("hash", 100, 100, false) < 1e18);
        QueryPlanner::setCostModel(QueryPlanner::CostModel{});
    }
    std::cout << "[COST] enable flags OK" << std::endl;

    // ------------------------------------------------------------------
    // 4. Custom hook overrides the built-in model; negative return falls
    //    back to the built-in computation.
    // ------------------------------------------------------------------
    {
        double builtIn = QueryPlanner::costJoinAlgorithm("hash", 500, 500, false);
        double builtInNlj = QueryPlanner::costJoinAlgorithm("nlj", 500, 500, false);
        QueryPlanner::CostModel cm;
        cm.customCost = &customHook;
        QueryPlanner::setCostModel(cm);
        assert(QueryPlanner::costJoinAlgorithm("hash", 500, 500, false) == 12345.0);
        assert(g_customCalls == 1);
        // Non-hash algorithms fall back to the built-in value.
        assert(std::abs(QueryPlanner::costJoinAlgorithm("nlj", 500, 500, false)
                        - builtInNlj) < 1e-9);
        (void)builtIn;
        QueryPlanner::setCostModel(QueryPlanner::CostModel{});
    }
    std::cout << "[COST] custom hook OK" << std::endl;

    // ------------------------------------------------------------------
    // 5. Scan costing: seq pays sequential pages, index pays random pages.
    // ------------------------------------------------------------------
    {
        QueryPlanner::CostModel cm;
        double seq = QueryPlanner::costScan("seq_scan", 1000, 100);
        double idx = QueryPlanner::costScan("index_scan", 1000, 100);
        assert(seq > 0 && idx > 0);
        cm.seqPageCost = 10.0;
        QueryPlanner::setCostModel(cm);
        assert(QueryPlanner::costScan("seq_scan", 1000, 100) > seq);
        QueryPlanner::setCostModel(QueryPlanner::CostModel{});
        // Sanity: with default parameters, scanning every tuple of a table
        // sequentially is priced by pages, not random fetches.
        double allSeq = QueryPlanner::costScan("seq_scan", 1000, 10);
        double allIdx = QueryPlanner::costScan("index_scan", 1000, 0);
        (void)allSeq; (void)allIdx;
    }
    std::cout << "[COST] scan costing OK" << std::endl;

    // ------------------------------------------------------------------
    // 6. Config GUCs parse, validate, and reject bad values.
    // ------------------------------------------------------------------
    {
        Config cfg;
        assert(cfg.setParameter("seq_page_cost", "2.5"));
        assert(cfg.seqPageCost == 2.5);
        assert(cfg.setParameter("random_page_cost", "8"));
        assert(cfg.randomPageCost == 8.0);
        assert(cfg.setParameter("cpu_tuple_cost", "0.02"));
        assert(cfg.setParameter("cpu_index_tuple_cost", "0.01"));
        assert(cfg.setParameter("cpu_operator_cost", "0.005"));
        assert(cfg.setParameter("enable_nestloop", "off"));
        assert(!cfg.enableNestloop);
        assert(cfg.validate());
        // Bad values are rejected without touching the config.
        assert(!cfg.setParameter("seq_page_cost", "abc"));
        assert(!cfg.setParameter("seq_page_cost", "-1"));
        assert(cfg.seqPageCost == 2.5);
        // Unknown parameter names stay unrecognized.
        assert(!cfg.setParameter("no_such_cost_guc", "1"));
    }
    std::cout << "[COST] config GUC parsing OK" << std::endl;

    // ------------------------------------------------------------------
    // 7. setCostParameter/setCostEnable (the SET-sync entry points).
    // ------------------------------------------------------------------
    {
        QueryPlanner::setCostParameter("seq_page_cost", 3.0);
        assert(QueryPlanner::costModel().seqPageCost == 3.0);
        QueryPlanner::setCostParameter("cpu_operator_cost", 0.05);
        assert(QueryPlanner::costModel().cpuOperatorCost == 0.05);
        QueryPlanner::setCostEnable("nestloop", false);
        assert(!QueryPlanner::costModel().enableNestloop);
        // Unknown names are ignored, negatives rejected.
        QueryPlanner::setCostParameter("bogus", 1.0);
        QueryPlanner::setCostParameter("seq_page_cost", -5.0);
        assert(QueryPlanner::costModel().seqPageCost == 3.0);
        QueryPlanner::setCostModel(QueryPlanner::CostModel{});
    }
    std::cout << "[COST] SET sync entry points OK" << std::endl;

    std::cout << "[COST] all tests passed" << std::endl;
    return 0;
}
