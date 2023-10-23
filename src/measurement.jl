@funsql begin

measurement() = begin
    from(measurement)
end

measurement_is_historical() = measurement_id > 1500000000
measurement_matches(ids...) = build_concept_matches($ids, measurement)
measurement_pairing(ids...) = build_concept_pairing($ids, measurement)
measurement_pivot(selection...; total=false, person_total=false, roundup=false) =
    build_pivot($selection, measurement, measurement_id,
                $total, $person_total, $roundup)
link_measurement(measurement=nothing) =
    link(measurement_date, $(something(measurement, @funsql measurement())))

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
