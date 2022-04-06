-- Workload.
-- Contains all filter columns.

-- Q35
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx,movie_companies mc
WHERE t.id=mi.movie_id AND t.id=mi_idx.movie_id AND t.id=mc.movie_id
AND mi_idx.info_type_id={movie_info_idx.info_type_id} AND mi.info_type_id={movie_info.info_type_id};

-- Q21
SELECT COUNT(*) FROM cast_info ci,title t
WHERE t.id=ci.movie_id
AND t.production_year>={title.production_year} AND t.production_year<={title.production_year} + 5;

-- Q5
SELECT COUNT(*) FROM movie_companies mc,title t,movie_keyword mk
WHERE t.id=mc.movie_id AND t.id=mk.movie_id
AND mk.keyword_id={movie_keyword.keyword_id};

-- Q69
SELECT COUNT(*) FROM title t,movie_info mi,movie_companies mc,cast_info ci,movie_keyword mk
WHERE t.id=mi.movie_id AND t.id=mc.movie_id AND t.id=ci.movie_id AND t.id=mk.movie_id
AND ci.role_id={cast_info.role_id} AND mi.info_type_id={movie_info.info_type_id}
AND t.production_year>={title.production_year} AND t.production_year<={title.production_year}+10 AND mk.keyword_id={movie_keyword.keyword_id};

-- Q64
SELECT COUNT(*) FROM title t,cast_info ci,movie_keyword mk,movie_info_idx mi_idx
WHERE t.id=mk.movie_id AND t.id=ci.movie_id AND t.id=mi_idx.movie_id
AND t.production_year>{title.production_year} AND t.kind_id={title.kind_id} AND mi_idx.info_type_id={movie_info_idx.info_type_id};

-- Q41
SELECT COUNT(*) FROM title t,movie_info mi,movie_companies mc,movie_keyword mk
WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.id=mc.movie_id
AND mi.info_type_id={movie_info.info_type_id} AND t.production_year>{title.production_year};

-- Q55
SELECT COUNT(*) FROM title t,movie_keyword mk,movie_companies mc,movie_info mi
WHERE t.id=mk.movie_id AND t.id=mc.movie_id AND t.id=mi.movie_id
AND mk.keyword_id={movie_keyword.keyword_id} AND mc.company_type_id={movie_companies.company_type_id} AND t.production_year>={title.production_year} AND t.production_year<={title.production_year} + 10;

-- Q25
SELECT COUNT(*) FROM cast_info ci,title t,movie_companies mc
WHERE t.id=ci.movie_id AND t.id=mc.movie_id
AND ci.role_id={cast_info.role_id};

-- Q39
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx,movie_keyword mk
WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.id=mi_idx.movie_id
AND t.kind_id={title.kind_id} AND mi.info_type_id={movie_info.info_type_id} AND mi_idx.info_type_id={movie_info_idx.info_type_id};

-- Q57
SELECT COUNT(*) FROM title t,movie_info mi,movie_info_idx mi_idx,movie_keyword mk,movie_companies mc
WHERE t.id=mi.movie_id AND t.id=mk.movie_id AND t.id=mi_idx.movie_id AND t.id=mc.movie_id
AND t.production_year>{title.production_year} AND mi.info_type_id={movie_info.info_type_id} AND mi_idx.info_type_id={movie_info_idx.info_type_id};