# Hadoop AI-native rebuild project

## Goal
We are building an AI native codebase rebuild pipeline. One experiment we want to do is that, given a large codebase, for example Hadoop, we want to first extract key features and dataflow out of it, and we want to pick the most important/essential ones, and then use coding agent to implement a "toy" Hadoop, which should realize the key functions of Hadoop (or MVP, but here we don't want to lose the performance, but just less features than Hadoop). Which are the key features tha we want to realize? Please review the structure of Hadoop and write a comprehensive report on that.

## Reference
- document of hadoop features and rebuild suggestions can be found under C:\Users\andyzeng\OneDrive - Microsoft\Documents\GitHub\mini-Hadoop\doc
- Hadoop's architectural summary"C:\Users\andyzeng\OneDrive - Microsoft\Documents\GitHub\Logical_inference\graph-wiki\compare-output\hadoop-architectural-summary.json"
- Hadoop vs Ozone report: "C:\Users\andyzeng\OneDrive - Microsoft\Documents\GitHub\Logical_inference\graph-wiki\compare-output\HADOOP_VS_OZONE_COMPARISON_REPORT.md" which might be useful
- Hadoop's graph index and retrieval: "C:\Users\andyzeng\OneDrive - Microsoft\Documents\GitHub\Logical_inference\graph-code-indexing\docs\hadoop-graph-indexing-report.md"

## Workflow
- First understand the Hadoop repo based on:
  - reference docs above
  - Hadoop codegraph (see below)
  - Hadoop features extraction (see below)
- Then decide which are the core features to implement. 
- Extract data flow, dependency graphs from the whole codegraph. Then understand the core business flows and API's.
- Based on all the information above, write a comprehensive implementation plan

## Tool to use
You should use our codegraph as as a powerful tool in this project, along with LSP:
- Graph indexing and search code: C:\Users\andyzeng\OneDrive - Microsoft\Documents\GitHub\Logical_inference\graph-code-indexing
- Hadoop's graph indexed and features: C:\Users\andyzeng\OneDrive - Microsoft\Documents\GitHub\Logical_inference\graph-code-indexing\output\hadoop (note:   Note: CALLS edges are available on the 4 subsets (152K total call edges) but skipped on the full repo to avoid OOM. The full repo has imports (87K) + inherits (7.8K) + parent_child (189K).)
  Hadoop Indexing 

  ┌──────────────────┬───────┬─────────┬─────────┬────────┬──────────┬─────────┬──────────┬───────┬────────┐
  │     Dataset      │ Files │  Nodes  │  Edges  │ Calls  │ Inherits │ Imports │ Packages │ Time  │  Size  │
  ├──────────────────┼───────┼─────────┼─────────┼────────┼──────────┼─────────┼──────────┼───────┼────────┤
  │ Full repo        │ 8,089 │ 201,125 │ 283,416 │ —      │ 7,803    │ 86,961  │ 46       │ 29min │ 264 MB │
  ├──────────────────┼───────┼─────────┼─────────┼────────┼──────────┼─────────┼──────────┼───────┼────────┤
  │ hadoop-common    │ 1,230 │ 18,469  │ 36,326  │ 15,494 │ 906      │ 2,687   │ 19       │ 19s   │ 58 MB  │
  ├──────────────────┼───────┼─────────┼─────────┼────────┼──────────┼─────────┼──────────┼───────┼────────┤
  │ hadoop-hdfs      │ 716   │ 16,019  │ 33,523  │ 16,083 │ 348      │ 1,789   │ —        │ 27s   │ 51 MB  │
  ├──────────────────┼───────┼─────────┼─────────┼────────┼──────────┼─────────┼──────────┼───────┼────────┤
  │ hadoop-mapreduce │ 1,010 │ 22,085  │ 49,404  │ 22,621 │ 712      │ 5,536   │ 17       │ 36s   │ 72 MB  │
  ├──────────────────┼───────┼─────────┼─────────┼────────┼──────────┼─────────┼──────────┼───────┼────────┤
  │ hadoop-yarn      │ 3,042 │ 67,491  │ 190,604 │ 98,137 │ 1,916    │ 27,175  │ —        │ 7min  │ 234 MB │
  └──────────────────┴───────┴─────────┴─────────┴────────┴──────────┴─────────┴──────────┴───────┴────────┘

  All Output Files (706 MB in graph-code-indexing/output/hadoop/)

  ┌────────────────────────────┬────────────┬───────────────────────────────────────────────────────────────────┐
  │            File            │    Size    │                              Content                              │
  ├────────────────────────────┼────────────┼───────────────────────────────────────────────────────────────────┤
  │ hadoop-data.json           │ 264 MB     │ Full repo graph: 201K nodes, 283K edges                           │
  ├────────────────────────────┼────────────┼───────────────────────────────────────────────────────────────────┤
  │ hadoop-features.json       │ 23 MB      │ Full repo features (124K nodes, 1.6M+ LOC)                        │
  ├────────────────────────────┼────────────┼───────────────────────────────────────────────────────────────────┤
  │ hadoop-packages.json       │ 4.4 KB     │ 46 Java packages — top: yarn (7K classes), hdfs (4.5K), fs (3.2K) │
  ├────────────────────────────┼────────────┼───────────────────────────────────────────────────────────────────┤
  │ hadoop-common-data.json    │ 58 MB      │ Common graph with CALLS edges                                     │
  ├────────────────────────────┼────────────┼───────────────────────────────────────────────────────────────────┤
  │ hadoop-hdfs-data.json      │ 51 MB      │ HDFS graph with CALLS edges                                       │
  ├────────────────────────────┼────────────┼───────────────────────────────────────────────────────────────────┤
  │ hadoop-mapreduce-data.json │ 72 MB      │ MapReduce graph with CALLS edges                                  │
  ├────────────────────────────┼────────────┼───────────────────────────────────────────────────────────────────┤
  │ hadoop-yarn-data.json      │ 234 MB     │ YARN graph with CALLS edges                                       │
  ├────────────────────────────┼────────────┼───────────────────────────────────────────────────────────────────┤
  │ *-features.json            │ 2.5-23 MB  │ Hierarchical features per subset                                  │
  ├────────────────────────────┼────────────┼───────────────────────────────────────────────────────────────────┤
  │ *-packages.json            │ 1.6-4.4 KB │ Java package analysis per subset                                  │
  └────────────────────────────┴────────────┴───────────────────────────────────────────────────────────────────┘

  Note: CALLS edges are available on the 4 subsets (152K total call edges) but skipped on the full repo to avoid OOM. The full repo has imports (87K) + inherits (7.8K) + parent_child (189K).
- Use graph-search skill if need to search/retrieval on the graph, which is more accurate and efficient than pure string search. Refer to "C:\Users\andyzeng\OneDrive - Microsoft\Documents\GitHub\Logical_inference\graph-code-indexing\docs\hadoop-graph-indexing-report.md" for details on our codebase graph's indexing and retrieval.

## Implementation
- Each time implements/modifies a feature, check with the graph or LSP for dependencies, make sure update related code accordingly and no conflicts

## Test
- We want to make sure we realize the core functions of Hadoop, for example, read/write, healthy run even with break down of one worker, etc. do researach online if necessary

## Review
- Use multiple review skills, including /pr-review /code-review, /review etc. to make sure we have a robust implementation
- We should review for each feature implementation, rather than wait until everything finished.