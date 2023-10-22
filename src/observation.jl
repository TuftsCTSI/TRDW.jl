@funsql begin

observation() = begin
    from(observation)
    define(is_historical => observation_id > 1500000000)
end

observation_matches(ids...) = build_concept_matches($ids, observation)
observation_pairing(ids...) = build_concept_pairing($ids, observation)

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
