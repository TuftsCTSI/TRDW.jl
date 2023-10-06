@funsql begin

measurement() = begin
    from(measurement)
end

measurement_isa(ids...) = is_descendant_concept(measurement_concept_id, $ids...)
measurement_type_isa(ids...) = is_descendant_concept(measurement_type_concept_id, $ids...)

join_measurement(ids...; carry=[]) = begin
    as(base)
    join(begin
        measurement()
        $(length(ids) == 0 ? @funsql(define()) :
            @funsql filter(is_descendant_concept(measurement_concept_id, $ids...)))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_measurement(ids...) = begin
	from(measurement)
	filter(person_id == :person_id)
    $(length(ids) == 0 ? @funsql(define()) :
        @funsql filter(is_descendant_concept(measurement_concept_id, $ids...)))
	bind(:person_id => person_id )
end

with_measurement_group(extension=nothing) =
    join(measurement_group => begin
      from(measurement)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == measurement_group.person_id)

end
