using TRDW

# TRDW's own, so test/Project.toml needn't list OrderedCollections.
const OrderedDict = TRDW.OrderedDict

# The notebook template's TRDW.json (ResearchRequests/template/TRDW.json),
# keeping only the fields TRDW reads. `pi_display_name` is left out:
# config_file() stores it as :pi_name, and nothing reads that.
#
# Who reads what:
#   project_name    config_file()[:project_code], with the "P-" stripped:
#                   user_project_schema(), project_schema(), subject_table()
#                   and ensure_subject_table() (src/subject.jl), templates
#   irb_id          is_discovery(), is_quality(), and the discovery/quality
#                   defaults in config_file(); templates
#   irb_start_date  irb_date_range(), is_during_irb_date_range(); templates
#   irb_end_date    same
#   project_id      templates (src/template.jl)
#   project_slug    templates
#   project_title   templates
#   description     templates
#
# This is the discovery project (IRB 11642), but every field the discovery
# defaults would fill in is given, so config_file() returns these values
# as written.
const TEST_CONFIG = OrderedDict{String, String}(
    "project_id" => "a0n8Y00000Z6YtIQAV",
    "project_name" => "P-005547",
    "irb_id" => "11642",
    "irb_start_date" => "2010-01-01",
    "irb_end_date" => "2029-12-31",
    "project_title" => "Tufts Research Data Warehouse (TRDW)",
    "project_slug" => "000000_Research_Request_Template",
    "description" => "This is for the general construction, improvement, maintenance, and documentation of the Tufts Research Data Warehouse (TRDW).",
)

# Environment variables TRDW reads while building queries. `nothing` unsets.
#   DATABRICKS_CATALOG              env_catalog() (src/general.jl)
#   DATABRICKS_TEMP_SCHEMA_PREFIX   temp_schema_prefix() (src/subject.jl);
#                                   unset, so the prefix is "zz_" * USER
#   USER, USERNAME                  same; USERNAME is the fallback
const TEST_ENV = (
    "DATABRICKS_CATALOG" => "ctsi",
    "DATABRICKS_TEMP_SCHEMA_PREFIX" => nothing,
    "USER" => "tester",
    "USERNAME" => nothing,
)

"""
    with_test_env(f; config = TEST_CONFIG, env = ())

Run `f()` with the environment variables in `TEST_ENV` (overridden by
`env`, one `name => value` pair or a tuple of them) and inside a new temporary
directory that holds a `TRDW.json` written from `config`. With
`config = nothing` there's no `TRDW.json`. The environment and the working
directory are restored afterward, also when `f` throws.

Build queries inside `f`, not only render them: some default arguments
(for example `schema = user_project_schema()`) are evaluated when the
function is called.
"""
function with_test_env(f; config = TEST_CONFIG, env = ())
    env isa Pair && (env = (env,))
    withenv(TEST_ENV..., env...) do
        mktempdir() do dir
            cd(dir) do
                if config !== nothing
                    open(TRDW.CONFIG_FILE, "w") do io
                        TRDW.JSON.print(io, config, 2)
                    end
                end
                f()
            end
        end
    end
end
