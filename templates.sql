-- Workload.
-- Contains all filter columns.
-- 1: Q35
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx,movie_companies mc
WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.id=mc.movie_id
AND mi_idx.info_type_id=0 AND mi.info_type_id=0;

-- 2: Q21
SELECT COUNT(*) FROM cast_info ci,title t
WHERE t.id=ci.movie_id
AND t.production_year BETWEEN 0 AND 10;

-- 3: Q5
SELECT COUNT(*) FROM movie_companies mc,title t,movie_keyword mk
WHERE t.id=mc.movie_id AND t.id=mk.movie_id
AND mk.keyword_id=0;

-- 4: Q69: modified with filter on mc.
SELECT COUNT(*) FROM title t,movie_info mi,movie_companies mc,cast_info ci,movie_keyword mk
WHERE t.id=mi.movie_id AND t.id=mc.movie_id AND t.id=ci.movie_id AND t.id=mk.movie_id
AND ci.role_id=0 AND mi.info_type_id=0
AND mk.keyword_id=0
AND t.production_year BETWEEN 0 AND 10
AND mc.company_type_id=0;

-- 5: Q64
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk,movie_info_idx mi_idx
WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.id=mi_idx.movie_id
AND t.kind_id=0 AND mi_idx.info_type_id=0
AND t.production_year BETWEEN 0 AND 30;

-- 6: Q41
SELECT COUNT(*) FROM title t,movie_info mi,movie_companies mc,movie_keyword mk
WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.id=mc.movie_id
AND mi.info_type_id=0
AND t.production_year BETWEEN 0 AND 50;

-- 7: Q55
SELECT COUNT(*) FROM title t,movie_keyword mk,movie_companies mc,movie_info mi
WHERE t.id=mk.movie_id AND t.id=mc.movie_id AND t.id=mi.movie_id
AND mk.keyword_id=0
AND mc.company_type_id=0
AND t.production_year BETWEEN 0 AND 30;

-- 8: Q25
SELECT COUNT(*) FROM cast_info ci,title t,movie_companies mc
WHERE t.id=ci.movie_id AND t.id=mc.movie_id
AND ci.role_id=0;

-- 9: Q39
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx,movie_keyword mk
WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.id=mi_idx.movie_id
AND t.kind_id=0
AND mi.info_type_id=0
AND mi_idx.info_type_id=0;

-- 10: Q57
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx,movie_keyword mk,movie_companies mc
WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.id=mi_idx.movie_id AND t.id=mc.movie_id
AND t.production_year BETWEEN 0 AND 30
AND mi.info_type_id=0
AND mi_idx.info_type_id=0;