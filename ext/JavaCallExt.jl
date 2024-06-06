module JavaCallExt

using TRDW

using JavaCall
using Markdown
using Pkg.Artifacts
using Tables
using Dates

const CellStyle = @jimport org.apache.poi.ss.usermodel.CellStyle
const CreationHelper = @jimport org.apache.poi.ss.usermodel.CreationHelper
const DataFormat = @jimport org.apache.poi.ss.usermodel.DataFormat
const EncryptionInfo = @jimport org.apache.poi.poifs.crypt.EncryptionInfo
const EncryptionMode = @jimport org.apache.poi.poifs.crypt.EncryptionMode
const Encryptor = @jimport org.apache.poi.poifs.crypt.Encryptor
const File = @jimport java.io.File
const FileOutputStream = @jimport java.io.FileOutputStream
const LocalDateTime = @jimport java.time.LocalDateTime
const OPCPackage = @jimport org.apache.poi.openxml4j.opc.OPCPackage
const OutputStream = @jimport java.io.OutputStream
const PackageAccess = @jimport org.apache.poi.openxml4j.opc.PackageAccess
const POIFSFileSystem = @jimport org.apache.poi.poifs.filesystem.POIFSFileSystem
const SXSSFCell = @jimport org.apache.poi.xssf.streaming.SXSSFCell
const SXSSFRow = @jimport org.apache.poi.xssf.streaming.SXSSFRow
const SXSSFSheet = @jimport org.apache.poi.xssf.streaming.SXSSFSheet
const SXSSFWorkbook = @jimport org.apache.poi.xssf.streaming.SXSSFWorkbook

function TRDW.XLSX.write(file, table; password = nothing)
    workbook = SXSSFWorkbook(())
    creation_helper = jcall(workbook, "getCreationHelper", CreationHelper, ())
    date_format = jcall(creation_helper, "createDataFormat", DataFormat, ())
    date_format_idx = jcall(date_format, "getFormat", jshort, (JString,), "yyyy-mm-dd")
    datetime_format = jcall(creation_helper, "createDataFormat", DataFormat, ())
    datetime_format_idx = jcall(datetime_format, "getFormat", jshort, (JString,), "yyyy-mm-ddThh:mm:ss")
    date_cell_style = jcall(workbook, "createCellStyle", CellStyle, ())
    jcall(date_cell_style, "setDataFormat", Nothing, (jshort,), date_format_idx)
    datetime_cell_style = jcall(workbook, "createCellStyle", CellStyle, ())
    jcall(datetime_cell_style, "setDataFormat", Nothing, (jshort,), datetime_format_idx)
    sheet = jcall(workbook, "createSheet", SXSSFSheet, (JString,), "Sheet1")
    for (i, t) in enumerate(Tables.schema(table).types)
        t !== nothing || continue
        t = Base.nonmissingtype(t)
        if t <: Dates.Date
            jcall(sheet, "setDefaultColumnStyle", Nothing, (jint, CellStyle), i-1, date_cell_style)
        elseif t <: Dates.DateTime
            jcall(sheet, "setDefaultColumnStyle", Nothing, (jint, CellStyle), i-1, datetime_cell_style)
        end
    end
    row = jcall(sheet, "createRow", SXSSFRow, (jint,), 0)
    for (i, c) in enumerate(Tables.columnnames(table))
        cell = jcall(row, "createCell", SXSSFCell, (jint,), i-1)
        jcall(cell, "setCellValue", Nothing, (JString,), string(c))
    end
    for (k, r) in enumerate(Tables.rows(table))
        row = jcall(sheet, "createRow", SXSSFRow, (jint,), k)
        vals = Any[Tables.getcolumn(r, c) for c in Tables.columnnames(r)]
        for (i, val) in enumerate(vals)
            cell = jcall(row, "createCell", SXSSFCell, (jint,), i-1)
            if val === missing
            elseif val isa Dates.Date
                datetime = jcall(LocalDateTime, "of", LocalDateTime, (jint, jint, jint, jint, jint), year(val), month(val), day(val), 0, 0)
                jcall(cell, "setCellValue", Nothing, (LocalDateTime,), datetime)
            elseif val isa Dates.DateTime
                datetime = jcall(LocalDateTime, "of", LocalDateTime, (jint, jint, jint, jint, jint, jint, jint), year(val), month(val), day(val), hour(val), minute(val), second(val), millisecond(val) * 1000000)
                jcall(cell, "setCellValue", Nothing, (LocalDateTime,), datetime)
            # elseif val isa Bool
            #     jcall(cell, "setCellValue", Nothing, (jboolean,), val)
            elseif val isa Number
                jcall(cell, "setCellValue", Nothing, (jdouble,), val)
            else
                jcall(cell, "setCellValue", Nothing, (JString,), string(val))
            end
        end
    end
    file_output_stream = FileOutputStream((JString,), file)
    jcall(workbook, "write", Nothing, (OutputStream,), file_output_stream)
    jcall(file_output_stream, "close", Nothing, ())
    password !== nothing || return
    filesystem = POIFSFileSystem(())
    agile_encryption_mode = jfield(EncryptionMode, "agile", EncryptionMode)
    encryption_info = EncryptionInfo((EncryptionMode,), agile_encryption_mode)
    encryptor = jcall(encryption_info, "getEncryptor", Encryptor, ())
    jcall(encryptor, "confirmPassword", Nothing, (JString,), password)
    read_write_package_access = jfield(PackageAccess, "READ_WRITE", PackageAccess)
    package = jcall(OPCPackage, "open", OPCPackage, (JString, PackageAccess), file, read_write_package_access)
    encrypted_stream = jcall(encryptor, "getDataStream", OutputStream, (POIFSFileSystem,), filesystem)
    jcall(package, "save", Nothing, (OutputStream,), encrypted_stream)
    jcall(encrypted_stream, "close", Nothing, ())
    jcall(package, "close", Nothing, ())
    file_output_stream = FileOutputStream((JString,), file)
    jcall(filesystem, "writeFilesystem", Nothing, (OutputStream,), file_output_stream)
    jcall(file_output_stream, "close", Nothing, ())
    jcall(filesystem, "close", Nothing, ())
    jcall(workbook, "dispose", jboolean, ())
    nothing
end

const CohortExpressionQueryBuilder = @jimport org.ohdsi.circe.cohortdefinition.CohortExpressionQueryBuilder
const BuildExpressionQueryOptions = @jimport org.ohdsi.circe.cohortdefinition.CohortExpressionQueryBuilder$BuildExpressionQueryOptions
const MarkdownRender = @jimport org.ohdsi.circe.cohortdefinition.printfriendly.MarkdownRender
const SqlRender = @jimport org.ohdsi.sql.SqlRender
const SqlTranslate = @jimport org.ohdsi.sql.SqlTranslate
const SqlSplit = @jimport org.ohdsi.sql.SqlSplit

function TRDW.OHDSI.cohort_definition_to_md(str)
    mr = MarkdownRender(())
    jcall(
        mr,
        "renderCohort",
        JString,
        (JString,),
        str) |> Markdown.parse
end

function TRDW.OHDSI.concept_set_list_definition_to_md(str)
    mr = MarkdownRender(())
    jcall(
        mr,
        "renderConceptSetList",
        JString,
        (JString,),
        str) |> Markdown.parse
end

function TRDW.OHDSI.concept_set_definition_to_md(str)
    mr = MarkdownRender(())
    jcall(
        mr,
        "renderConceptSet",
        JString,
        (JString,),
        str) |> Markdown.parse
end

function TRDW.OHDSI.cohort_definition_to_sql_template(str)
    builder = CohortExpressionQueryBuilder(())
    jcall(
        builder,
        "buildExpressionQuery",
        JString,
        (JString, BuildExpressionQueryOptions),
        str,
        nothing)
end

function TRDW.OHDSI.render_sql(template, params = (;))
    jcall(
        SqlRender,
        "renderSql",
        JString,
        (JString, Vector{JString}, Vector{JString}),
        template,
        collect(String, string.(keys(params))),
        collect(String, string.(values(params))))
end

function TRDW.OHDSI.translate_sql(sql; dialect = "spark", session_id = nothing, temp_emulation_schema = nothing)
    jcall(
        SqlTranslate,
        "translateSql",
        JString,
        (JString, JString, JString, JString),
        sql,
        dialect,
        session_id !== nothing ? string(session_id) : nothing,
        temp_emulation_schema !== nothing ? string(temp_emulation_schema) : nothing)
end

function TRDW.OHDSI.split_sql(sql)
    v = jcall(
        SqlSplit,
        "splitSql",
        Vector{JString},
        (JString,),
        sql)
    map(JavaCall.unsafe_string, v)
end

function __init__()
    JavaCall.addClassPath(joinpath(artifact"csv2xlsx", "*"))
    JavaCall.addClassPath(joinpath(artifact"CirceR", "CirceR-1.3.2/inst/java/*"))
    JavaCall.addClassPath(joinpath(artifact"SqlRender", "SqlRender-1.16.1/inst/java/*"))
end

end
