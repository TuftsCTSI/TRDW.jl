# The function and aggregate names `spark.jl` exports only render their
# own names, so only `>>` has cases here.

const SPARK_CASES = [
    "a >> b, b a name" =>
        () -> @funsql(person().define(a => named_struct("a", 1) >> a)),
    "a >> b, b a String" =>
        () -> @funsql(person().define(a => named_struct("a", 1) >> $("a"))),
]

@testset "spark.jl" begin
    @testset "$name renders" for (name, build) in SPARK_CASES
        @test with_test_env(() -> build_and_render(build, context = name)) !== nothing
    end

    @testset ">> renders as field access" begin
        sql = render_ok(@funsql person().define(a => named_struct("a", 1) >> a))
        @test sql !== nothing && occursin("named_struct('a', 1).a", sql.raw)
    end
end
