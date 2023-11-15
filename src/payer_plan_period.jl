@funsql begin

payer_plan_period(match...) = begin
	from(payer_plan_period)
	$(length(match) == 0 ? @funsql(define()) : @funsql(filter(payer_matches($match))))
	left_join(concept => concept(),
		payer_concept_id == concept.concept_id, optional=true)
	left_join(plan_concept => concept(),
		 plan_concept_id == concept.concept_id, optional=true)
	left_join(source_concept => concept(),
              payer_source_concept_id == source_concept.concept_id, optional=true)
	join(event => begin
		from(payer_plan_period)
		define(
		table_name => "payer_plan_period",
		concept_id => payer_concept_id,
		end_date => payer_plan_period_end_date,
		start_date => payer_plan_period_start_date,
            source_concept_id => payer_source_concept_id)
    end, payer_plan_period_id == event.payer_plan_period_id, optional = true)
end

payer_matches(match...) = concept_matches($match; match_prefix=payer)

end
