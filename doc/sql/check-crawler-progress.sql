-- Where the running crawler stands, read from its resume server variables
-- (README, "Resume"): current family, current process, start time, and the
-- last processed id of each family. Read-only. Run twice a few minutes apart
-- to see how fast the current family advances.

SELECT NOW() AS NOW_, UTC_TIMESTAMP() AS UTC_NOW;

SELECT VAR_NAME, VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE VAR_NAME LIKE 'strwikipediacrawler%'
   OR VAR_NAME LIKE 'strwikipediadumpingest%'
ORDER BY TIM_UPDATED DESC, VAR_NAME;
