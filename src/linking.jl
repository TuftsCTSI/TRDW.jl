""" join_via_cohort(name => query, date_prefix; match, match_prefix, match_source, mandatory)

This constructs an outer join that links via person and cohort date under the given name.
"""
function join_via_cohort(pair::Pair{Symbol, FunSQL.SQLNode}, date_prefix=nothing;
                         match=[], exclude=nothing,
                         match_prefix= nothing, match_source=nothing,
                         mandatory=false)
    (nest_name, query) = pair
    date_prefix = something(date_prefix, nest_name)
    match_prefix = something(match_prefix, date_prefix)
    start_date = contains(string(date_prefix), "_date") ? date_prefix :
        Symbol("$(date_prefix)_start_date")
    end_date = contains(string(date_prefix), "_date") ? date_prefix :
        Symbol("$(date_prefix)_end_date")
    return @funsql(begin
        left_join($nest_name => begin
            $query
            $(length(match) == 0 ? @funsql(define()) :
              @funsql filter(concept_matches($match; match_prefix=$match_prefix,
                                             match_source=$match_source)))
            $(isnothing(exclude) ? @funsql(define()) :
              @funsql filter(!concept_matches($exclude; match_prefix=$match_prefix,
                                             match_source=$match_source)))
        end, person_id == $nest_name.person_id &&
             coalesce($nest_name.$end_date, $nest_name.$start_date) >= cohort_start_date &&
             $nest_name.$start_date <= cohort_end_date)
        $(mandatory ? @funsql(filter(not(is_null($nest_name.person_id)))) : @funsql(define()))
    end)
end
const var"funsql#join_via_cohort" = join_via_cohort

""" join_via_cohort(query, date_prefix; match, match_prefix, match_source, carry)

This constructs an inner join that continues the current frame with the joined
query as filtered by person and cohort date. The carry parameter can be used to
bring base columns into the new context.
"""
function join_via_cohort(query::FunSQL.SQLNode, date_prefix::Symbol;
                         match=nothing, exclude=nothing,
                         match_prefix= nothing, match_source=nothing,
                         carry=nothing)
    base = gensym()
    match_prefix = something(match_prefix, date_prefix)
    start_date = contains(string(date_prefix), "_date") ? date_prefix :
        Symbol("$(date_prefix)_start_date")
    end_date = contains(string(date_prefix), "_date") ? date_prefix :
        Symbol("$(date_prefix)_end_date")
    return @funsql(begin
        as($base)
        join($query, person_id == $base.person_id)
        define(cohort_start_date => $base.cohort_start_date)
        define(cohort_end_date => $base.cohort_end_date)
        filter(coalesce($end_date, $start_date) >= cohort_start_date &&
               $start_date <= cohort_end_date)
        $(isnothing(match) || length(match) == 0 ? @funsql(define()) :
          @funsql filter(concept_matches($match; match_prefix=$match_prefix,
                                         match_source=$match_source)))
        $(isnothing(exclude) || length(exclude) == 0 ? @funsql(define()) :
          @funsql filter(!concept_matches($exclude; match_prefix=$match_prefix,
                                          match_source=$match_source)))
        define($([@funsql($n => $base.$n) for n in something(carry,[])]...))
    end)
end

""" correlate_via_cohort(query, date_prefix; match, match_prefix, match_source)

This constructs a correlated query that links via person and cohort date.
"""
function correlate_via_cohort(query::FunSQL.SQLNode, date_prefix::Symbol;
                              match=[], exclude=nothing, also=nothing,
                              match_prefix= nothing, match_source=nothing)
    match_prefix = something(match_prefix, date_prefix)
    start_date = contains(string(date_prefix), "_date") ? date_prefix :
        Symbol("$(date_prefix)_start_date")
    end_date = contains(string(date_prefix), "_date") ? date_prefix :
        Symbol("$(date_prefix)_end_date")
    return @funsql(begin
        $query
        filter(person_id == :person_id &&
               coalesce($end_date, $start_date) >= :cohort_start_date &&
               $start_date <= :cohort_end_date)
        $(isnothing(also) ? @funsql(define()) : also)
        $(length(match) == 0 ? @funsql(define()) :
          @funsql filter(concept_matches($match; match_prefix=$match_prefix,
                                         match_source=$match_source)))
        $(isnothing(exclude) ? @funsql(define()) :
          @funsql filter(!concept_matches($exclude; match_prefix=$match_prefix,
                                          match_source=$match_source)))
        bind(:person_id => person_id,
             :cohort_start_date => cohort_start_date,
             :cohort_end_date => cohort_end_date)
    end)
end
const var"funsql#correlate_via_cohort" = correlate_via_cohort

""" join_via_person(name => query; match, match_prefix, match_source, prefix)

This constructs an outer join that links via person under the given name.
"""
function join_via_person(pair::Pair{Symbol, FunSQL.SQLNode};
                         match=[], exclude=nothing,
                         match_prefix= nothing, match_source=nothing,
                         mandatory=false)
    (nest_name, query) = pair
    match_prefix = coalesce(match_prefix, nest_name)
    return @funsql(begin
        left_join($nest_name => begin
            $query
            $(length(match) == 0 ? @funsql(define()) :
              @funsql filter(concept_matches($match; match_prefix=$match_prefix,
                                             match_source=$match_source))),
            $(isnothing(exclude) ? @funsql(define()) :
              @funsql filter(concept_matches($exclude; match_prefix=$match_prefix,
                                             match_source=$match_source))),
            person_id == $nest_name.person_id
        end)
        $(mandatory ? @funsql(filter(not(is_null($nest_name.person_id)))) : @funsql(define()))
    end)
end
const var"funsql#join_via_person" = join_via_person

""" join_via_person(query; match, match_prefix, match_source, carry)

This constructs an inner join that continues the current frame with the joined
query as filtered by person. The carry parameter can be used to bring base columns
into the new context.
"""
function join_via_person(query::FunSQL.SQLNode;
                         match=[], exclude=nothing,
                         match_prefix= nothing, match_source=nothing,
                         carry=[])
    base = gensym()
    return @funsql(begin
        as($base)
        join($query, person_id == $base.person_id)
        define($([@funsql($n => $base.$n) for n in carry]...))
        $(length(match) == 0 ? @funsql(define()) :
          @funsql filter(concept_matches($match; match_prefix=$match_prefix,
                                         match_source=$match_source)))
        $(isnothing(exclude) ? @funsql(define()) :
          @funsql filter(concept_matches($exclude; match_prefix=$match_prefix,
                                         match_source=$match_source)))
    end)
end

""" correlate_via_person(query; match, match_prefix, match_source)

This constructs a correlated query that links via person.
"""
function correlate_via_person(query::FunSQL.SQLNode, date_prefix::Symbol;
                              match=[], exclude=nothing,
                              match_prefix= nothing, match_source=nothing)
    return @funsql(begin
        $query
        filter(person_id == :person_id)
        $(length(match) == 0 ? @funsql(define()) :
          @funsql filter(concept_matches($match; match_prefix=$match_prefix,
                                         match_source=$match_source)))
        $(isnothing(exclude) ? @funsql(define()) :
          @funsql filter(concept_matches($exclude; match_prefix=$match_prefix,
                                         match_source=$match_source)))
        bind(:person_id => person_id)
    end)
end
const var"funsql#correlate_via_person" = correlate_via_person
