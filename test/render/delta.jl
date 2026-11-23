# Render cases for src/delta.jl
#
# delta.jl reads its own `ctsi.<schema>.<table>` tables, not the catalog's.

# The delta functions that take `schema` and `reference_schema` with no
# defaults. Their docstrings call them with no arguments.
const DELTA_NO_DEFAULTS = [
    :delta_care_site,
    :delta_condition_occurrence,
    :delta_observation,
    :delta_measurement,
    :delta_observation_period,
]

delta_fn(name) = getfield(TRDW, Symbol("funsql_", name))

const DELTA_CASES = [
    "delta_ctes(table, schema, reference_schema, columns)" =>
        () -> @funsql(begin
            from(latest_person)
            join(previous => from(previous_person), person_id == previous.person_id)
            delta_ctes("person", "trdw_prime_latest", "trdw_prime_previous", $([:person_id]))
        end),
    "delta_person(schema, reference_schema)" =>
        () -> @funsql(delta_person("trdw_prime_2026_11", "trdw_prime_2026_10")),
    "person_snapshot_delta(schema, reference_schema)" =>
        () -> @funsql(person_snapshot_delta("trdw_prime_2026_11", "trdw_prime_2026_10")),
    "snapshot_delta(schema, reference_schema)" =>
        () -> @funsql(snapshot_delta("trdw_prime_2026_11", "trdw_prime_2026_10")),
    [
        "$name(schema, reference_schema)" =>
            () -> delta_fn(name)("trdw_prime_latest", "trdw_prime_previous")
        for name in DELTA_NO_DEFAULTS
    ]...,
]

@testset "delta.jl" begin
    @testset "$name renders" for (name, build) in DELTA_CASES
        @test with_test_env(() -> build_and_render(build, context = name)) !== nothing
    end

    @testset "delta_person reads the given schemas" begin
        sql = render_ok(@funsql delta_person("trdw_prime_2026_11", "trdw_prime_2026_10"))
        @test sql !== nothing && occursin("`ctsi`.`trdw_prime_2026_11`.`person`", sql.raw)
        @test sql !== nothing && occursin("`ctsi`.`trdw_prime_2026_10`.`person`", sql.raw)
    end

    @testset "$name() renders as documented" for name in DELTA_NO_DEFAULTS
        @test with_test_env(() -> build_and_render(delta_fn(name), context = "$name()")) !== nothing
    end
end
