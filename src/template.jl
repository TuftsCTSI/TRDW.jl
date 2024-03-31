const DISCOVERY_IRB = "11642"

const wide_notebook_style = html"""
<style>
/*    @media screen and (min-width: calc(700px + 25px + 283px + 34px + 25px)) */
        main {
            margin: 0 auto;
            max-width: 2000px;
            padding-right: 50px;
        }
</style>
"""

WideStyle() =
    wide_notebook_style

is_discovery(irb) = isnothing(irb) || string(irb) == DISCOVERY_IRB

NotebookFooter(;CASE=nothing, SFID=nothing, IRB=DISCOVERY_IRB) = @htl("""
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

NotebookHeader(TITLE=nothing, STUDY=nothing; CASE=nothing, SFID=nothing,
               IRB=DISCOVERY_IRB, IRB_START_DATE=nothing, IRB_END_DATE=nothing) = @htl("""
   <div style="overflow: auto; width: 100%; vertical-align: top;">
     <h1 style="display: inline-block; width: 88%; text-align: left; vertical-align: top;">
       $(TITLE)
       $(isnothing(STUDY) ? "" :
         @htl("""<p style="font-style: italic; font-size: 21px;">$STUDY</p>"""))
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
            <p>IRB Study # $(IRB) ($IRB_START_DATE to $IRB_END_DATE)</p>
        """)
     end)
""")
