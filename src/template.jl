const CONFIG_FILE = "TRDW.json"
const DISCOVERY_IRB = "11642"
const CONFIGURATION = Dict{String, Any}()

is_discovery(irb) = isnothing(irb) || string(irb) == DISCOVERY_IRB

function configuration()
    if length(CONFIGURATION) == 0 && isfile(CONFIG_FILE)
        merge!(CONFIGURATION, JSON.parsefile(CONFIG_FILE))
    end
    return CONFIGURATION
end

function configuration(TITLE, NOTE, CASE, SFID, IRB)
    config = configuration()
    if "case" in keys(config)
        case = config["case"]
        SFID = isnothing(SFID) ? get(case, "slug", nothing) : SFID
        CASE = isnothing(CASE) ? get(case, "id", nothing) : CASE
        TITLE = isnothing(TITLE) ? get(case, "title", nothing) : TITLE
    end
    if "project" in keys(config)
        project = config["project"]
        IRB = isnothing(IRB) ? get(project, "irb", DISCOVERY_IRB) : IRB
        NOTE = isnothing(NOTE) ? get(project, "title", nothing) : NOTE
    end
    return (TITLE, NOTE, CASE, SFID, IRB)
end

case_id() = configuration()["case"]["id"]

function NotebookFooter(;CASE=nothing, SFID=nothing, IRB=nothing)
  (TITLE, NOTE, CASE, SFID, IRB) = configuration(nothing, nothing, CASE, SFID, IRB)
  @htl("""
  <div>
    <table style="width: 100%">
    <tr><td style="width: 72; vertical-align: top">
      <small>
        Produced by <a href="https://www.tuftsctsi.org/">
        Tufts Clinical and Translational Science Institute (CTSI) Informatics.</a><br/>
        Cite NIH CTSA Award UM1TR004398 when using Tufts CTSI resources.<br/>
        Generated at $(Dates.now())
      </small>
    </td><td style="width: 28%; vertical-align: top; text-align: right;">
    $(isnothing(CASE) ? "" : @htl("""Service Request#
      $(isnothing(SFID) ? @htl("""<span>$CASE</span>""") : @htl("""
        <a href="https://tuftsctsi.lightning.force.com/lightning/r/Case/$SFID/view">$CASE</a>
        """))
      <br />"""))
    $(is_discovery(IRB) ? "" : @htl("<p>IRB Study# $(IRB)"))
  </div>
   """)
end

function NotebookHeader(TITLE=nothing; NOTE=nothing, CASE=nothing, SFID=nothing,
                        IRB=nothing, IRB_START_DATE=nothing, IRB_END_DATE=nothing)
  (TITLE, NOTE, CASE, SFID, IRB) = configuration(TITLE, NOTE, CASE, SFID, IRB)
  @htl("""
   <!-- wide notebooks -->
   <style>
           main {
               margin: 0 auto;
               max-width: 2000px;
               padding-right: 50px;
           }
   </style>
   <div style="overflow: auto; width: 100%; vertical-align: top;">
     <h1 style="display: inline-block; width: 88%; text-align: left; vertical-align: top;">
       $(TITLE)
     </h1>
     <div style="display: inline-block; width: 11%; text-align: right;
                 height: 100%; vertical-align: middle;">
       $(isnothing(CASE) ? "" : @htl("""Service Request#
          $(isnothing(SFID) ? @htl("""<span>$CASE</span>""") : @htl("""
            <a style="text-decoration: underline dotted;"
               href="https://tuftsctsi.my.site.com/s/case/$SFID ">$CASE</a>
        """))"""))
     </div>
   </div>
   $(isnothing(NOTE) ? "" :
     @htl("""<p style="font-style: italic; font-size: 21px;">$NOTE</p>"""))
   $(if is_discovery(IRB)
        @htl("""
            <p>
                This cohort discovery is provided under IRB Protocol #11642,
                <i>"Accelerating Clinical Trials - Multi-institutional cohort discovery"</i>,
                permitting <i>"aggregate, obfuscated patient counts"</i>.
                Counts below ten are indicated with the ≤ symbol.
            </p>
        """)
     else
        @htl("""
            <p>IRB Study # $(IRB)
            $(isnothing(IRB_START_DATE) ? "" : @htl("($IRB_START_DATE to $IRB_END_DATE)"))
            </p>
        """)
     end)
   $(PlutoUI.TableOfContents())
  """)
end
