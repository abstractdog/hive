#!/bin/bash
for i in `seq 1 1000`; do
	echo "***************************** $i **************************"
	mvn -q test -T 1C -Dtest.output.overwrite=true -Pitests,hadoop-2 -pl itests/qtest -pl itests/util -Dtest=TestMiniLlapLocalCliDriver -Dqfile=dynamic_semijoin_reduction.q,materialized_view_create_rewrite_3.q,vectorization_pushdown.q,correlationoptimizer2.q,cbo_gby_empty.q,schema_evol_text_nonvec_part_all_complex_llap_io.q,vectorization_short_regress.q,mapjoin3.q,cross_product_check_1.q,results_cache_quoted_identifiers.q,unionDistinct_3.q,cbo_join.q,correlationoptimizer6.q,union_remove_26.q,cbo_rp_limit.q,convert_decimal64_to_decimal.q,vector_groupby_cube1.q,union2.q,groupby2.q,dynpart_sort_opt_vectorization.q,constraints_optimization.q,exchgpartition2lel.q,retry_failure.q,schema_evol_text_vecrow_part_llap_io.q,sample10.q,vectorized_timestamp_ints_casts.q,auto_sortmerge_join_2.q,bucketizedhiveinputformat.q,cte_mat_2.q,vectorization_8.q
done
