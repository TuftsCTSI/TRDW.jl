# Run from the package root with
#
#     julia +1.11.6 --project=test test/runtests.jl
#

using Test
using TRDW

include("support/mock_catalog.jl")
include("support/environment.jl")
include("support/render.jl")

@testset "TRDW" begin
    @testset "loads" begin
        @test isdefined(TRDW, :funsql_person)
    end
    @testset "test helpers" begin
        include("selftest/mock_catalog.jl")
        include("selftest/environment.jl")
        include("selftest/render.jl")
    end
    @testset "render" begin
        include("render/zero_args.jl")
    end
end
