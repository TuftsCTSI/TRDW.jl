uses(text) = [(u.name, u.line) for d in funsql_definitions(text) for u in d.uses]
used_names(text) = [u.name for d in funsql_definitions(text) for u in d.uses]

@testset "funsql_definitions" begin
    @testset "a default that names another parameter" begin
        # antijoin before it was fixed
        @test uses("""
            @funsql begin
            antijoin(q, lhs, rhs=lhs) = begin
                 left_join(_antijoin => \$q, \$lhs == _antijoin.\$rhs)
                 filter(isnull(_antijoin.\$rhs))
                 undefine(_antijoin)
            end
            end
            """) == [(:lhs, 2)]
        @test isempty(used_names("""
            @funsql antijoin(q, lhs, rhs=\$lhs) = begin
                 left_join(_antijoin => \$q, \$lhs == _antijoin.\$rhs)
                 filter(isnull(_antijoin.\$rhs))
            end
            """))
    end

    @testset "a parameter passed on bare" begin
        @test used_names("""
            @funsql join_first(joinee, on; order_by) =
                join_first(joinee; on = \$on, order_by = \$order_by)
            """) == [:joinee]
    end

    @testset "a bare parameter as a field name" begin
        @test used_names("@funsql f(q, key) = filter(isnull(other.key))") == [:key]
        @test isempty(used_names("@funsql f(q, key) = filter(isnull(other.\$key))"))
    end

    @testset "keyword names aren't uses" begin
        @test isempty(used_names("@funsql f(q; on, name) = join(\$q, on = \$on, name = \$name)"))
        @test isempty(used_names("@funsql f(by) = partition(frame = (mode = rows, by = 1))"))
    end

    @testset "a column compared with the parameter of the same name" begin
        @test isempty(used_names("@funsql f(relationship_id) = filter(relationship_id == \$relationship_id)"))
        @test used_names("@funsql f(relationship_id) = filter(relationship_id == 1)") == [:relationship_id]
    end

    @testset "a parameter name also bound as an alias" begin
        @test isempty(used_names("""
            @funsql f(source, target) = begin
                \$source
                into(source)
                join(target => \$target, source.id == target.id)
            end
            """))
    end

    @testset "a function name isn't a use" begin
        @test isempty(used_names("@funsql f(count) = define(n => count())"))
    end

    @testset "an output name isn't a use" begin
        @test isempty(used_names("@funsql f(n) = define(n => count())"))
        @test isempty(used_names("@funsql f(name) = define(\$name => count())"))
        @test used_names("@funsql f(n) = define(total => n)") == [:n]
    end

    @testset "Julia functions that build queries" begin
        @test used_names("""
            function funsql_f(x)
                y = x + 1
                @funsql filter(x > \$y)
            end
            """) == [:x]
        @test used_names("""
            funsql_g(x) = begin
                y = x + 1
                @funsql filter(\$x > y)
            end
            """) == [:y]
        @test isempty(used_names("""
            function funsql_f(x)
                @funsql filter(column > \$x)
            end
            """))
        # A function without @funsql isn't a definition here.
        @test isempty(funsql_definitions("f(x) = x + 1"))
    end

    @testset "comprehension variables" begin
        @test used_names("@funsql f(by) = and(args = \$[@funsql(key == 1) for key in by])") == [:key]
        @test isempty(used_names("@funsql f(by) = and(args = \$[@funsql(\$key == 1) for key in by])"))
    end

    @testset "definitions in a block, with docstrings" begin
        defs = funsql_definitions("""
            @funsql begin

            \"\"\"
                f(x)
            \"\"\"
            f(x) = filter(x > 1)

            g(x) = filter(\$x > 1)

            end
            """)
        @test [d.name for d in defs] == [:f, :g]
        @test [d.line for d in defs] == [6, 8]
        @test [[u.name for u in d.uses] for d in defs] == [[:x], []]
    end
end

@testset "included_sources" begin
    mktempdir() do dir
        write(joinpath(dir, "Main.jl"), """
            module M
            include("a.jl")
            #include("b.jl")
            include("c.jl")
            end
            """)
        @test included_sources(joinpath(dir, "Main.jl")) ==
            joinpath.(dir, ["Main.jl", "a.jl", "c.jl"])
    end
end
