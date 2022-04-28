COPY cast_info FROM 'data/job_light/trimmed/5_cast_info.csv' (FORMAT 'csv', quote '"', delimiter ',', header 0);
COPY title FROM 'data/job_light/trimmed/1_title.csv' (FORMAT 'csv', quote '"', delimiter ',', header 0);
COPY movie_companies FROM 'data/job_light/trimmed/4_movie_companies.csv' (FORMAT 'csv', quote '"', delimiter ',', header 0);
COPY movie_info_idx FROM 'data/job_light/trimmed/3_movie_info_idx.csv' (FORMAT 'csv', quote '"', delimiter ',', header 0);
COPY movie_keyword FROM 'data/job_light/trimmed/2_movie_keyword.csv' (FORMAT 'csv', quote '"', delimiter ',', header 0);
COPY movie_info FROM 'data/job_light/trimmed/0_movie_info.csv' (FORMAT 'csv', quote '"', delimiter ',', header 0);
