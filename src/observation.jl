@funsql begin

observation() = begin
    from(observation)
end

observation_isa(ids...) = is_descendant_concept(observation_concept_id, $ids...)
observation_type_isa(ids...) = is_descendant_concept(observation_type_concept_id, $ids...)

correlated_observation(ids...) = begin
	from(observation)
	filter(person_id == :person_id)
	filter(is_descendant_concept(observation_concept_id, $ids...))
	bind(:person_id => person_id )
end

with_observation_group(extension=nothing) =
    join(observation_group => begin
      from(observation)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == observation_group.person_id)

end
