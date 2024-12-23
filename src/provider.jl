@funsql begin

provider(match...) =
    provider().filter(concept_matches($match))

specialty_concept() = if_defined_scalar(provider,
                                        provider.omop.specialty_concept_id,
	                                    omop.specialty_concept_id)
specialty_isa(args...) =
    category_isa($Specialty, $args, specialty_concept())

end
