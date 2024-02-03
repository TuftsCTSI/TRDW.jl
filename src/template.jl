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

NotebookFooter(;case=nothing, sfid=nothing, pims=nothing) = @htl("""
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
    $(isnothing(pims) ? "" : @htl("""PIMS #
        <a href="https://pims.tuftsctsi.org/issues/$(pims)">$(pims)</a>"""))
    </td></tr></table>
  </div>
""")

NotebookHeader(title=nothing, subtitle=nothing; case=nothing, sfid=nothing) = @htl("""
   <h1>$(title)
   </h1>
  $(isnothing(case) ? "" : @htl("""
    <div style="text-align: center;">Service Request#
    $(isnothing(sfid) ? @htl("""<span>$case</span>""") : @htl("""
      <a href="https://tuftsctsi.my.site.com/s/case/$sfid/">$case</a>
      """))
    </div>"""))
   $(isnothing(subtitle) ? "" :
     @htl("""<p style="text-align: left; font-style: italic; font-size: 24px;">$subtitle</p>"""))
""")
