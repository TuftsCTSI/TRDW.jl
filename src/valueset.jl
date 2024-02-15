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


function parse_valuesets(s)
    xdoc = LightXML.parse_string(s)
    try
        nothing
    finally
        LightXML.free(xdoc)
    end
    ttt = """
        {
            "id" : "2.16.840.1.113883.3.117.1.7.1.226",
            "name" : "Thrombolytic tPA Therapy",
            "purpose" : "Clinical Focus: The purpose of this value set is to represent concepts",
            "concepts" : [
                {
                    "code":"1804799",
                    "codeSystemName":"RXNORM",
                    "displayName":"alteplase 100 MG Injection"
                },
                {
                    "code":"1804804",
                    "codeSystemName":"RXNORM",
                    "displayName":"alteplase 50 MG Injection"
                },
                {
                    "code":"313212",
                    "codeSystemName":"RXNORM",
                    "displayName":"tenecteplase 50 MG Injection"
                }
            ]
        }
    """
    anem = """
        {
            "id" : "2.16.840.1.113762.1.4.1251.23",
            "name" : "Anemia",
            "purpose" : "Clinical Focus: The purpose of this value set is to represent concepts",
            "concepts" : [
                {
                    "code":"10205009",
                    "codeSystemName":"SNOMEDCT",
                    "displayName":"Megaloblastic anemia due to exfoliative dermatitis (disorder)"
                },
                {
                    "code":"105599000",
                    "codeSystemName":"SNOMEDCT",
                    "displayName":"Anemia related to disturbed deoxyribonucleic acid synthesis (disorder)"
                },
                {
                    "code":"10564005",
                    "codeSystemName":"SNOMEDCT",
                    "displayName":"Severe hereditary spherocytosis due to combined deficiency of spectrin AND ankyrin (disorder)"
                }
                
            ]
        }
    """
    [JSON.parse(ttt), JSON.parse(anem)]


end

function resolve_valuesets!(valuesets)
    valuesets[1]["concepts"][1]["concept"] = nothing#RxNorm(1804799)
    valuesets[1]["concepts"][2]["concept"] = RxNorm(1804804)
    valuesets[1]["concepts"][3]["concept"] = RxNorm(313212)
    valuesets[2]["concepts"][1]["concept"] = SNOMED(10205009)
    valuesets[2]["concepts"][2]["concept"] = SNOMED(105599000)
    valuesets[2]["concepts"][3]["concept"] = nothing
end

function valuesets(oids)

    # TODO: check cache here
    # algorithm: for each id, look to see if there is a cached xml response
    # if yes, parse and add it to the results

    oids_to_fetch = oids # minus cahced values

    res = fetch_valuesets([p for p in oids])

    ans = parse_valuesets(String(res.body))
    
    resolve_valuesets!(ans)

    csets = []

    htmls = []

    for oid in oids
        for a in ans
            if a["id"] == oid
                push!(csets, [c["concept"] for c in a["concepts"] if !isnothing(c["concept"])])
                push!(htmls, @htl("""
                    <dl>
                        <dt>Name</dt>
                        <dd>$(a["name"])</dd>
                        <dt>ID</dt>
                        <dd>$(a["id"])</dd>
                        <dt>Purpose</dt>
                        <dd>$(a["purpose"])</dd>
                        <dt>Concepts</dt>
                        <dd>
                        <table>
                            <tr>
                                <th>Resolved</th>
                                <th>Vocabulary</th>
                                <th>Code</th>
                                <th>Display Name</th>
                            </tr>
                            $([@htl("""
                                <tr>
                                    <td>
                                        $(isnothing(c["concept"]) ? "No" : "Yes")
                                    </td>
                                    <td>
                                        $(c["codeSystemName"])
                                    </td>
                                    <td>
                                        $(c["code"])
                                    </td>
                                    <td>
                                        $(c["displayName"])
                                    </td>
                                </tr>
                            """) for c in a["concepts"]])
                        </table>
                        </dd>
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

function valueset(oid, name=nothing)

    res = fetch_valuesets([oid])

    ans = parse_valuesets(String(res.body))
    
    resolve_valuesets!(ans)

    ans = ans[1]

    if !isnothing(name) && ans["name"] != name
        error("Provided name does not match name of Valueset ($(ans["name"])).")
    end
   
    ret = Concept[]
    for a in ans["concepts"]
        if any(isnothing(a["concept"])) 
            error("Valueset $oid has unresolved concept ($(a["codeSystemName"]), $(a["code"]))")
        end
        push!(ret, a["concept"])
    end
    
    return ret
end

macro valuesets(expr::Expr)
    @assert expr.head==:block
    oids = []
    len = convert(Int, (length(expr.args) / 2))
    for idx in range(1, len)
        item = expr.args[idx * 2]
        @assert item.head==:(=)
        (name, oid) = item.args
        item.args[1] = esc(name)
        push!(oids, item.args[2])
    end
    (htmls, csets) = valuesets(oids)
    push!(expr.args, HTML(htmls))
    for (idx, val) in enumerate(csets)
        expr.args[idx * 2].args[2] = val
    end
    return expr
end
