namespace Etl;

public static class StringAssets
{
    public static string clientfaceSQL { get; set; } = """
    ;WITH [CteFaceList] AS (
        SELECT gf.[Face_id]
        FROM dbo.[glbFace] gf WITH (NOLOCK)
        INNER JOIN dbo.[pthFaceBase] fb WITH (NOLOCK) ON fb.[FaceBase_id] = gf.[FaceBase_id]
        --WHERE gf.[ChangeDateTime] BETWEEN ISNULL(@StartDate, 0) AND ISNULL(@EndDate, '30000101')
        UNION
        SELECT gf.[Face_id]
        FROM dbo.[glbFace] gf WITH (NOLOCK)
        INNER JOIN dbo.[pthFaceBase] fb WITH (NOLOCK) ON fb.[FaceBase_id] = gf.[FaceBase_id]
        --WHERE [fb].[ChangeDateTime] BETWEEN ISNULL(@StartDate, 0) AND ISNULL(@EndDate, '30000101')
    ),
    CL AS (
        SELECT [client_id] = gf.[Face_id],
                [name] = CAST(n.[NameFull] AS VARCHAR(512)),
                [doc_number] = d.[Number],
                [doc_serial] = d.[Serial],
                [doc_date] = d.[IssueDate],
                [doc_issuer] = CAST(d.[Issuer] AS VARCHAR(255)),
                [doc_issuer_code] = d.[IssuerCode],
                [birth_day] = fb.[BirthDay],
                [sex] = CAST(CASE fb.[Sex_id]
                                    WHEN 1 THEN 'M'
                                    WHEN 2 THEN 'F'
                                    ELSE CONVERT(VARCHAR, fb.[Sex_id])
                            END AS CHAR(1)
                            ),
                [type] = CAST(CASE fb.[FaceType_id]
                                    WHEN 1 THEN 'F'
                                    ELSE 'U'
                            END AS CHAR(1)
                            ),     
                [is_examined] = gf.[IsExamined],
                [status] = CAST(1 AS TINYINT),
                [SourceCreateDateTime] = gf.[CreateDateTime],
                [SourceChangeDateTime] = gf.[ChangeDateTime],
                [DocumentType_id] = d.[DocumentType_id],
                [ClientUID] = gf.[FaceUID],
                [DateProfInvestor] = CAST(x.[DateTimeCreated] AS DATE),
                [ClientTargetUID] = CAST(null AS UNIQUEIDENTIFIER),
                [NameFirst] = n.[NameFirst],
                [NameMiddle] = n.[NameMiddle],
                [NameLast] = n.[NameLast],
                [ClientINN] = y.[Inn]
            FROM [CteFaceList] t WITH (NOLOCK)
            INNER JOIN dbo.[glbFace] gf WITH (NOLOCK) ON gf.[Face_id] = t.[Face_id]
            INNER JOIN dbo.[pthFaceBase] fb WITH (NOLOCK) ON fb.[FaceBase_id] = gf.[FaceBase_id]
            INNER JOIN dbo.[pthName] n WITH (NOLOCK) ON n.[Name_id] = fb.[Name_id]
            INNER JOIN dbo.[pthDocument] d WITH (NOLOCK) ON d.[Document_id] = fb.[Document_id]
            OUTER APPLY ( -- дата первого документа подтверждающего квалификацию инвестора
            SELECT TOP 1 xq.[DateTimeCreated]
            FROM [Pythoness_buf].common.[PythiaInvestor] xt WITH (NOLOCK)
            INNER JOIN [Pythoness_buf].QUAL.[PythiaDocumentRel] xq WITH (NOLOCK) ON xq.[Investor_id] = xt.[PythiaInvestor_id]
            WHERE xt.[Face_id] = gf.[Face_id]
            ORDER BY xq.[DateTimeCreated] ASC
            ) x
            OUTER APPLY ( -- инн инвестора
            SELECT TOP 1 yt.[Inn]
            FROM [Pythoness_buf].common.[PythiaInvestor] AS [yt] WITH (NOLOCK)
            INNER JOIN [Pythoness_buf].[QUAL].[PythiaDocumentRel] xq WITH (NOLOCK) ON xq.[Investor_id] = yt.[PythiaInvestor_id]
            WHERE yt.[Face_id] = gf.[Face_id]
                    AND NULLIF(yt.[Inn], '') IS NOT NULL
            ORDER BY xq.[DateTimeCreated] ASC
            ) y

        UNION ALL

        SELECT [client_id] = [fd].[Face_id],
                [name] = NULL,
                [doc_number] = NULL,
                [doc_serial] = NULL,
                [doc_date] = NULL,
                [doc_issuer] = NULL,
                [doc_issuer_code] = NULL,
                [birth_day] = NULL,
                [sex] = NULL,
                [type] = NULL,
                [is_examined] = fd.[IsExamined],
                [status] = CAST(0 AS TINYINT),
                [SourceCreateDateTime] = fd.[CreateDateTime],
                [SourceChangeDateTime] = fd.[ChangeDateTime],
                [DocumentType_id] = CAST(0 AS SMALLINT),
                [ClientUID] = fd.[FaceUID],
                [DateProfInvestor] = CAST(null as date),
                [ClientTargetUID] = fd.[FaceUIDTarget],
                [NameFirst] = NULL,
                [NameMiddle] = NULL,
                [NameLast] = NULL,
                [ClientINN] = NULL
            FROM dbo.[glbFace_deleted] fd WITH (NOLOCK)
            LEFT JOIN dbo.[glbFace] gf WITH (NOLOCK) ON gf.[Face_id] = fd.[Face_id]
            WHERE --fd.[DeleteDateTime] BETWEEN ISNULL(@StartDate, 0) AND ISNULL(@EndDate, '30000101')
                --AND 
                gf.[Face_id] IS NULL
        ),
    [res] AS (
            SELECT [RowNumber] = ROW_NUMBER() OVER(PARTITION BY [client_id] ORDER BY [status] DESC),
                    [client_id],
                    [name],
                    [doc_number],
                    [doc_serial],
                    [doc_date],
                    [doc_issuer],
                    [doc_issuer_code],
                    [birth_day],
                    [sex],
                    [type],
                    [is_examined],
                    [status],
                    [SourceCreateDateTime],
                    [SourceChangeDateTime],
                    [DocumentType_id],
                    [ClientUID],
                    [DateProfInvestor],
                    [ClientTargetUID],
                    [NameFirst],
                    [NameMiddle],
                    [NameLast],
                    [ClientINN]
            FROM [cl]
        )
        SELECT [client_id],
                [name],
                [doc_number],
                [doc_serial],
                [doc_date],
                [doc_issuer],
                [doc_issuer_code],
                [birth_day],
                [sex],
                [type],
                [is_examined],
                [status],
                [SourceCreateDateTime],
                [SourceChangeDateTime],
                [DocumentType_id],
                [ClientUID],
                [DateProfInvestor],
                [ClientTargetUID],		  
                [NameFirst],
                [NameMiddle],
                [NameLast],
                [ClientINN]
        FROM [res]
        WHERE [RowNumber] = 1; -- избавляемся от дублей при грязном чтении
    """;
}
