// https://sfc-gh-dwilczak.github.io/tutorials/snowflake/git/introduction/

------CONNECT REPO---------
---------------------------
use role accountadmin;
use poppy;
use schema poppy.secrets;

create or replace secret github_secret
    type = password
    username = 'jayozer' 
    password = '<github_token>'; 

create or replace api integration git_api_integration
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/jayozer') 
    allowed_authentication_secrets = (github_secret)
    enabled = true;

create or replace git repository pk_snowflake
    api_integration = git_api_integration
    git_credentials = github_secret
    origin = 'https://github.com/jayozer/pk_snowflake';

//DROP git repository pk_snowflake;


------NAVIGATE REPO---------
---------------------------
-- Show repos added to snowflake.
show git repositories;

-- Show branches in the repo.
show git branches in git repository pk_snowflake;

-- List files.
ls @pk_snowflake/branches/main;

-- Show code in file.
select $1 from @pk_snowflake/branches/main/examples/pk_google_reviews_analytics.ipynb;

-- Fetch git repository updates.
alter git repository pk_snowflake fetch;

SHOW GIT REPOSITORIES;


