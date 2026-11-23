# Test `funsql_*` exports that can be called with no arguments
# Exports with no zero-argument method and `spark.jl` closures are skipped.

is_spark_closure(x) = x isa Union{FunSQL.FunClosure, FunSQL.AggClosure}

has_zero_arg_method(f::Function) =
    any(m -> m.nargs == 1 || (m.isva && m.nargs == 2), methods(f))
has_zero_arg_method(f) = true

const EXPORTS = sort!([
    name for name in names(TRDW)
    if startswith(String(name), "funsql_") && !is_spark_closure(getfield(TRDW, name))])

const NEEDS_INPUT = Pair{Symbol, Function}[
    # src/omop.jl queries that expect `datetime`
    :funsql_take_earliest => () -> @funsql(condition()),
    :funsql_take_latest => () -> @funsql(condition()),
    # src/omop.jl concept set combinators
    :funsql_deduplicate_concepts => () -> @funsql(concept()),
    :funsql_include_descendant_concepts => () -> @funsql(concept()),
    :funsql_exclude_descendant_concepts => () -> @funsql(concept()),
    :funsql_include_prefix_descendant_concepts => () -> @funsql(concept()),
    :funsql_exclude_prefix_descendant_concepts => () -> @funsql(concept()),
    :funsql_map_to_standard_concepts => () -> @funsql(concept()),
    # src/subject.jl queries that join the person map on person_id or fact_id_1/_2
    :funsql_define_subject_id => () -> @funsql(person()),
    :funsql_use_subject_id => () -> @funsql(person()),
    :funsql_fact_use_subject_id => () -> @funsql(from(fact_relationship)),
    # src/nodes.jl column summaries
    :funsql_density => () -> @funsql(person()),
    :funsql_summary => () -> @funsql(person()),
    :funsql_summarize => () -> @funsql(person()),
]

const NOT_ZERO_ARG = Pair{Symbol, String}[
    :funsql_CPT4 => "needs `spec`",
    :funsql_ICD10CM => "needs `spec`",
    :funsql_define_before => "needs `name`",
    :funsql_define_after => "needs `name`",
    :funsql_if_set => "needs `name`",
    :funsql_take_first => "needs `order_by`",
    :funsql_assert_valid_concept => "needs `condition`",
    :funsql_try_get => "needs `names`",

    :funsql_write_xlsx => "needs a file name and at least one sheet",
    :funsql_write_encrypted_xlsx => "needs a file name and at least one sheet",

    :funsql_custom_resolve => "ill-formed without arguments",

    :funsql_config_file => "returns a DataFrame; test/selftest/environment.jl",
    :funsql_export => "exports TRDW's funsql_ names; returns nothing",
    :funsql_is_during_irb_date_range => "returns a name => condition pair; test/selftest/environment.jl",
    :funsql_project_schema => "returns a String; test/selftest/environment.jl",
    :funsql_user_project_schema => "returns a String; test/selftest/environment.jl",
    :funsql_subject_table => "returns a SQLTable; test/selftest/environment.jl",
    :funsql_truncate_subject_table => "returns a run() specification",
    :funsql_noop => "returns an ETL transform",
    :funsql_set => "returns an ETL transform",
    :funsql_unset => "returns an ETL transform",
    :funsql_unset_all_except => "returns an ETL transform",
    :funsql_snapshot => "returns an ETL transform",
    :funsql_format => "the type FormatSQLQuery",
]

const LISTED = union(first.(NEEDS_INPUT), first.(NOT_ZERO_ARG))

const ZERO_ARG = [
    name for name in EXPORTS
    if !(name in LISTED) && has_zero_arg_method(getfield(TRDW, name))]

label(name) = String(name)[length("funsql_") + 1:end] * "()"

export_fn(name) = getfield(TRDW, name)

quietly(f) = Base.CoreLogging.with_logger(f, Base.CoreLogging.NullLogger())

@testset "zero-argument exports" begin
    @testset "lists" begin
        @test !isempty(ZERO_ARG)
        @test allunique(vcat(first.(NEEDS_INPUT), first.(NOT_ZERO_ARG)))
        @testset "$(label(name)) is listed correctly" for name in LISTED
            @test name in EXPORTS
            @test name in EXPORTS && has_zero_arg_method(export_fn(name))
        end
    end

    @testset "$(label(name)) renders" for name in ZERO_ARG
        @test with_test_env(() -> build_and_render(export_fn(name), context = label(name))) !== nothing
    end

    @testset "$(label(name)) renders on an input" for (name, start) in NEEDS_INPUT
        f = export_fn(name)
        @test with_test_env(() -> build_and_render(() -> start() |> f(), context = label(name))) !== nothing
        @test quietly(() -> with_test_env(() -> build_and_render(f))) === nothing
    end

    @testset "$(label(name)) still can't render alone" for (name, reason) in NOT_ZERO_ARG
        @test quietly(() -> with_test_env(() -> build_and_render(export_fn(name)))) === nothing
    end
end
