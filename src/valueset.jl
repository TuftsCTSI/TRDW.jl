smoking_behavior_concepts() = [
	    OMOP_Extension("OMOP5181846","Cigar smoker"),
        OMOP_Extension("OMOP5181838","Cigarette smoker"),
        OMOP_Extension("OMOP5181836","Electronic cigarette smoker"),
        OMOP_Extension("OMOP5181847","Hookah smoker"),
        OMOP_Extension("OMOP5181837","Passive smoker"),
        OMOP_Extension("OMOP5181845","Pipe smoker")]

@funsql smoking_behavior_concepts() = concept($(smoking_behavior_concepts())...)

@funsql matches_smoking_behavior() =
    concept_matches($(smoking_behavior_concepts()); match_on=value_as)
