struct UnprintableError <: Exception end
Base.showerror(::IO, ::UnprintableError) = error("Unable to display this error.")

@testset "render_ok" begin
    @test render_ok(@funsql person()) isa FunSQL.SQLString
    sql = @test_logs (:error, r"render failed: .*no_such_column") render_ok(
        @funsql from(person).select(no_such_column))
    @test sql === nothing
    @test_logs (:error, r"printing it threw ErrorException") print_render_error(UnprintableError())
    @test_logs (:error, r"^test case: render failed: ") render_ok(
        @funsql(from(person).select(no_such_column)), context = "test case")
end

@testset "build_and_render" begin
    @test build_and_render(() -> @funsql person()) isa FunSQL.SQLString
    @test (@test_logs (:error, r"^test case: building the query failed: .*Query construction failed\.") build_and_render(
        () -> error("Query construction failed."), context = "test case")) === nothing
    @test (@test_logs (:error, r"built a String, not a FunSQL.SQLQuery") build_and_render(
        () -> "SELECT 1")) === nothing
    @test (@test_logs (:error, r"render failed: .*no_such_column") build_and_render(
        () -> @funsql from(person).select(no_such_column))) === nothing
end
