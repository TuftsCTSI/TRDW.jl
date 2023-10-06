@funsql begin

observation() = begin
    from(observation)
end

observation_isa(ids...) = is_descendant_concept(observation_concept_id, $ids...)
observation_type_isa(ids...) = is_descendant_concept(observation_type_concept_id, $ids...)

join_observation(ids...; carry=[]) = begin
    as(base)
    join(begin
        observation()
        $(length(ids) == 0 ? @funsql(define()) :
            @funsql filter(is_descendant_concept(observation_concept_id, $ids...)))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_observation(ids...) = begin
	from(observation)
	filter(person_id == :person_id)
    $(length(ids) == 0 ? @funsql(define()) :
        @funsql filter(is_descendant_concept(observation_concept_id, $ids...)))
	bind(:person_id => person_id )
end

with_observation_group(extension=nothing) =
    join(observation_group => begin
      from(observation)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == observation_group.person_id)

end
