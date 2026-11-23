# Render cases for src/nodes.jl

# A `custom_resolve` that leaves its input as it is.
passthrough() = TRDW.funsql_custom_resolve((n, ctx) -> FunSQL.resolve(ctx))

const NODES_CASES = [
    "define_front(args...)" =>
        () -> @funsql(person().define_front(n => 1)),
    "define_before(args...; name)" =>
        () -> @funsql(person().define_before(n => 1, name = gender_concept_id)),
    "define_after(args...; name)" =>
        () -> @funsql(person().define_after(n => 1, name = gender_concept_id)),
    "undefine(names...)" =>
        () -> @funsql(person().undefine(birth_datetime, death_datetime)),
    "try_get(names...), first name present" =>
        () -> @funsql(person().define(born => try_get(birth_datetime, no_such_column))),
    "try_get(names...), first name missing" =>
        () -> @funsql(person().define(born => try_get(no_such_column, birth_datetime))),
    "try_get(names...), with a Regex" =>
        () -> @funsql(person().define(born => try_get($(r"^birth_")))),
    "try_get(names...) on a joined query" =>
        () -> @funsql(person().left_join(v => visit(), person_id == v.person_id).define(start => v.try_get(no_such_column, datetime))),
    "explain_concept_id(args...)" =>
        () -> @funsql(person().explain_concept_id(concept_name, concept_code)),
    "explain_concept_id(; replace = true)" =>
        () -> @funsql(person().explain_concept_id(replace = true)),
    "summary(names...)" =>
        () -> @funsql(person().summary(birth_datetime, gender_concept_id)),
    "summary(; extrema, top_k, nested, private, exact)" =>
        () -> @funsql(person().summary(type = false, extrema = true, top_k = 3, nested = true, private = true, exact = true)),
    "summary(; exact = true), as the template uses it" =>
        () -> @funsql(from(cohort).summary(exact = true)),
    "density(names...)" =>
        () -> @funsql(person().density(gender_concept_id)),
    "summarize(names...)" =>
        () -> @funsql(person().summarize(gender_concept_id)),
    "_summary_approx_top(q, k, i)" =>
        () -> @funsql(person().group().define(top => _summary_approx_top(gender_concept_id, 3, 1))),
    "count_all(; include)" =>
        () -> @funsql(count_all(include = $(r"^visit"))),
    "count_all(; exclude, filter)" =>
        () -> @funsql(count_all(exclude = $(r"^previous_"), filter = person_id == 1)),
    "custom_resolve(resolve)" =>
        () -> @funsql(person().$(passthrough())),
    "custom_resolve(; resolve_scalar, terminal)" =>
        () -> @funsql(person().define(n => $(TRDW.funsql_custom_resolve(
            resolve_scalar = (n, ctx) -> @funsql(1), terminal = true)))),
    "if_set(name, query, else_query), name set" =>
        () -> @funsql(if_set(previous_cohort, from(previous_cohort), from(cohort).filter(false))),
    "if_set(name, query), on an input" =>
        () -> @funsql(from(cohort).if_set(previous_cohort, filter(true))),
    "if_defined(field_name, q), field defined" =>
        () -> @funsql(person().if_defined(birth_datetime, filter(is_not_null(birth_datetime)))),
    "if_defined(field_name, q, else_q), field not defined" =>
        () -> @funsql(person().if_defined(no_such_column, filter(false), define(no_such_column => missing))),
    "if_defined(field_name, q), field not defined" =>
        () -> @funsql(person().if_defined(no_such_column, filter(false))),
    "if_not_defined(field_name, q), field not defined" =>
        () -> @funsql(person().if_not_defined(age, define(age => 0))),
    "if_not_defined(field_name, q), field defined" =>
        () -> @funsql(person().if_not_defined(birth_datetime, filter(false))),
    "if_defined_scalar(field_name, q, else_q)" =>
        () -> @funsql(person().define(
            born => if_defined_scalar(birth_datetime, birth_datetime, missing),
            age => if_defined_scalar(no_such_column, no_such_column, missing))),
    "assert_valid_concept(condition, ex)" =>
        () -> @funsql(concept().filter(assert_valid_concept(concept_id == 8507, $(:(Gender("M")))))),
]

# Cases that render against a catalog with no `previous_cohort`.
const NODES_NO_PREVIOUS_CASES = [
    "if_set(name, query, else_query), name not set" =>
        () -> @funsql(if_set(previous_cohort, from(previous_cohort), from(cohort).filter(false))),
    "if_set(name, query), on an input, name not set" =>
        () -> @funsql(from(cohort).if_set(previous_cohort, filter(false))),
]

@testset "nodes.jl" begin
    @testset "$name renders" for (name, build) in NODES_CASES
        @test with_test_env(() -> build_and_render(build, context = name)) !== nothing
    end
    @testset "$name renders" for (name, build) in NODES_NO_PREVIOUS_CASES
        @test with_test_env(() -> build_and_render(build, catalog = mock_catalog(previous = false), context = name)) !== nothing
    end

    @testset "if_set picks its branch by the catalog" begin
        q = @funsql if_set(previous_cohort, from(previous_cohort), from(cohort))
        sql = render_ok(q)
        @test sql !== nothing && occursin("`previous_cohort`", sql.raw)
        sql = render_ok(q, catalog = mock_catalog(previous = false))
        @test sql !== nothing && !occursin("`previous_cohort`", sql.raw)
    end

    @testset "try_get fails when no name is present" begin
        @test_throws FunSQL.ReferenceError FunSQL.render(
            MOCK_CATALOG, @funsql(person().define(born => try_get(no_such_column, other_column))))
    end

    @testset "custom_resolve without a resolve function is ill-formed" begin
        @test_throws FunSQL.IllFormedError FunSQL.render(
            MOCK_CATALOG, @funsql(person().$(TRDW.funsql_custom_resolve())))
    end

    # FunSQL prints a node, and an error whose path contains it, through
    # `quoteof`. (A query built with `@funsql` prints its source instead.)
    @testset "a custom_resolve query prints" begin
        @test sprint(show, passthrough()) isa String
        err = try
            FunSQL.render(MOCK_CATALOG, @funsql(person().$(TRDW.funsql_custom_resolve())))
        catch err
            err
        end
        @test sprint(showerror, err) isa String
    end
end
