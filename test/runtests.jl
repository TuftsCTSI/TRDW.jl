# Run from the package root with
#
#     julia +1.11.6 --project=test test/runtests.jl
#

using Test
using TRDW

include("support/mock_catalog.jl")

@testset "TRDW" begin
    @testset "loads" begin
        @test isdefined(TRDW, :funsql_person)
    end
    @testset "render" begin
        include("render/mock_catalog.jl")
    end
end
