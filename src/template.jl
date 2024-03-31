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

NotebookFooter(;case=nothing, sfid=nothing) = @htl("""
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
    $(isnothing(case) ? "" : @htl("""Service Request#
      $(isnothing(sfid) ? @htl("""<span>$case</span>""") : @htl("""
        <a href="https://tuftsctsi.lightning.force.com/lightning/r/Case/$sfid/view">$case</a>
        """))
      <br />"""))
    </td></tr></table>
  </div>
""")

NotebookHeader(title=nothing, subtitle=nothing; case=nothing, sfid=nothing, irb=nothing) = @htl("""
   <div style="overflow: auto; width: 100%; vertical-align: top;">
     <h1 style="display: inline-block; width: 88%; text-align: left; vertical-align: top;">
       $(title)
       $(isnothing(subtitle) ? "" :
         @htl("""<p style="font-style: italic; font-size: 21px;">$subtitle</p>"""))
     </h1>
     <div style="display: inline-block; width: 11%; text-align: right;
                 height: 100%; vertical-align: middle;">
       $(isnothing(case) ? "" : @htl("""Service Request#
          $(isnothing(sfid) ? @htl("""<span>$case</span>""") : @htl("""
            <a style="text-decoration: underline dotted;"
               href="https://tuftsctsi.my.site.com/s/case/$sfid/">$case</a>
        """))"""))
    </div>
   </div>
   $(if isnothing(irb) || irb == 11642
        @htl("""
            <p>
                This cohort discovery is provided under IRB Protocol #11642,
                <i>"Accelerating Clinical Trials - Multi-institutional cohort discovery"</i>,
                permitting <i>"aggregate, obfuscated patient counts"</i>.
                Counts below ten are indicated with the ≤ symbol.
            </p>
        """)
     end)
""")
