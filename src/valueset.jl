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

function fetch_valuesets(ids)
    valueset_ids = join([HTTP.URIs.escapeuri(strip(x)) for x in ids], ",")
    apikey = ENV["UMLS_APIKEY"]
    valueset_url = "https://vsac.nlm.nih.gov/vsac/svs/RetrieveMultipleValueSets?id=$valueset_ids"
    encoded_apikey = HTTP.Base64.base64encode("apikey:$apikey")
    headers = ["Authorization" => "Basic $encoded_apikey"]
    HTTP.get(valueset_url, headers)
end

function parse_valuesets(s::String)
    xdoc = LightXML.parse_string(s)
    try
        nothing
    finally
        LightXML.free(xdoc)
    end
    [
        (
            id = "2.16.840.1.113883.3.117.1.7.1.226",
            name = "Thrombolytic tPA Therapy",
            purpose = "Clinical Focus: The purpose of this value set is to represent concepts",
            concepts = [
                RxNorm(1804799),
                RxNorm(1804804),
                RxNorm(313212)
            ]
        ),
        (
            id = "2.16.840.1.113762.1.4.1251.23",
            name = "Anemia",
            purpose = "Clinical Focus: The purpose of this value set is to represent concepts",
            concepts = [
                SNOMED(10205009),
                SNOMED(105599000),
                SNOMED(10564005)
            ]
        )
    ]
end

function valuesets(pairs::Pair...)

    res = fetch_valuesets([p[2] for p in pairs])

    ans = parse_valuesets(String(res.body))
    
    csets = Pair[]

    htmls = []

    for (name, id) in pairs
        for a in ans
            if a.id == id
                push!(csets, name => a.concepts)
                push!(htmls, @htl("""
                    <dl>
                        <dt>Name</dt>
                        <dd>$(a.name)</dd>
                        <dt>ID</dt>
                        <dd>$(a.id)</dd>
                        <dt>Purpose</dt>
                        <dd>$(a.purpose)</dd>
                    </dl>
                """))
            end
        end 
    end 
    htmls = @htl("""
        <br>
        $htmls
    """)
    return (htmls, csets)
end

macro valuesets(expr::Expr)
        @assert expr.head == :(=)
        (name, query) = expr.args
        sname = esc(string(name))
        vname = esc(name)
        return quote
            $vname = TRDW.run($db, @funsql $query.to_subject_id($case).order(subject_id))
            htmls
        end
    end
# valuesets(tpa_therapy => "2.16...", anemia => "2.....23")

# (tpa_therapy=[...], anemia=[...])