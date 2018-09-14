-- reconcile product vs quantity
select
	s1.'0' as s1_key,
	s2.'0' as s2_key,
	s1.'1' as s1_value1,
	s2.'1' as s2_value1,
	(s1.'1' - s2.'1') as s1s2_value1_diff
from system1 s1
join row_map on s1.'0' = row_map.system1
join system2 s2 on s2.'0' = row_map.system2;
