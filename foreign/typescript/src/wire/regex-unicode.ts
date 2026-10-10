// Unicode property names regex-syntax 0.8.11 (Unicode 16.0.0) accepts,
// copied from its tables. Each entry is a canonical name, optionally followed
// by "=" and its other normalized aliases separated by "|". The normalized
// canonical name is always an alias too.

export const PROPERTY_NAMES =
  "Age,ASCII_Hex_Digit=ahex,Alphabetic=alpha,Bidi_Class=bc,Bidi_Control=bidic,Bidi_Mirrored=b" +
  "idim,Bidi_Mirroring_Glyph=bmg,Bidi_Paired_Bracket=bpb,Bidi_Paired_Bracket_Type=bpt,Block=b" +
  "lk,Canonical_Combining_Class=ccc,Cased,Case_Folding=cf,Case_Ignorable=ci,Composition_Exclu" +
  "sion=ce,Changes_When_Casefolded=cwcf,Changes_When_Casemapped=cwcm,Changes_When_Lowercased=" +
  "cwl,Changes_When_NFKC_Casefolded=cwkcf,Changes_When_Titlecased=cwt,Changes_When_Uppercased" +
  "=cwu,kAccountingNumeric=cjkaccountingnumeric,kCompatibilityVariant=cjkcompatibilityvariant" +
  ",kIICore=cjkiicore,kIRG_GSource=cjkirggsource,kIRG_HSource=cjkirghsource,kIRG_JSource=cjki" +
  "rgjsource,kIRG_KPSource=cjkirgkpsource,kIRG_KSource=cjkirgksource,kIRG_MSource=cjkirgmsour" +
  "ce,kIRG_SSource=cjkirgssource,kIRG_TSource=cjkirgtsource,kIRG_UKSource=cjkirguksource,kIRG" +
  "_USource=cjkirgusource,kIRG_VSource=cjkirgvsource,kOtherNumeric=cjkothernumeric,kPrimaryNu" +
  "meric=cjkprimarynumeric,kRSUnicode=cjkrsunicode|unicoderadicalstroke|urs,Full_Composition_" +
  "Exclusion=compex,Dash,Decomposition_Mapping=dm,Decomposition_Type=dt,Default_Ignorable_Cod" +
  "e_Point=di,Deprecated=dep,Diacritic=dia,East_Asian_Width=ea,Emoji_Modifier_Base=ebase,Emoj" +
  "i_Component=ecomp,Emoji_Modifier=emod,Emoji,Emoji_Presentation=epres,Equivalent_Unified_Id" +
  "eograph=equideo,Expands_On_NFC=xonfc,Expands_On_NFD=xonfd,Expands_On_NFKC=xonfkc,Expands_O" +
  "n_NFKD=xonfkd,Extender=ext,Extended_Pictographic=extpict,FC_NFKC_Closure=fcnfkc,General_Ca" +
  "tegory=gc,Grapheme_Cluster_Break=gcb,Grapheme_Base=grbase,Grapheme_Extend=grext,Grapheme_L" +
  "ink=grlink,Hangul_Syllable_Type=hst,Hex_Digit=hex,Hyphen,ID_Continue=idc,ID_Compat_Math_Co" +
  "ntinue,ID_Compat_Math_Start,Ideographic=ideo,ID_Start=ids,IDS_Binary_Operator=idsb,IDS_Tri" +
  "nary_Operator=idst,IDS_Unary_Operator=idsu,Indic_Conjunct_Break=incb,Indic_Positional_Cate" +
  "gory=inpc,Indic_Syllabic_Category=insc,ISO_Comment=isc,Jamo_Short_Name=jsn,Joining_Group=j" +
  "g,Join_Control=joinc,Joining_Type=jt,kEH_Cat,kEH_Desc,kEH_HG,kEH_IFAO,kEH_JSesh,kEH_NoMirr" +
  "or,kEH_NoRotate,Line_Break=lb,Lowercase_Mapping=lc,Logical_Order_Exception=loe,Lowercase=l" +
  "ower,Math,Modifier_Combining_Mark=mcm,Name=na,Unicode_1_Name=na1,Name_Alias,Noncharacter_C" +
  "ode_Point=nchar,NFC_Quick_Check=nfcqc,NFD_Quick_Check=nfdqc,NFKC_Casefold=nfkccf,NFKC_Quic" +
  "k_Check=nfkcqc,NFKC_Simple_Casefold=nfkcscf,NFKD_Quick_Check=nfkdqc,Numeric_Type=nt,Numeri" +
  "c_Value=nv,Other_Alphabetic=oalpha,Other_Default_Ignorable_Code_Point=odi,Other_Grapheme_E" +
  "xtend=ogrext,Other_ID_Continue=oidc,Other_ID_Start=oids,Other_Lowercase=olower,Other_Math=" +
  "omath,Other_Uppercase=oupper,Pattern_Syntax=patsyn,Pattern_White_Space=patws,Prepended_Con" +
  "catenation_Mark=pcm,Quotation_Mark=qmark,Radical,Regional_Indicator=ri,Sentence_Break=sb,S" +
  "cript=sc,Simple_Case_Folding=scf|sfc,Script_Extensions=scx,Soft_Dotted=sd,Sentence_Termina" +
  "l=sterm,Simple_Lowercase_Mapping=slc,Simple_Titlecase_Mapping=stc,Simple_Uppercase_Mapping" +
  "=suc,White_Space=space|wspace,Titlecase_Mapping=tc,Terminal_Punctuation=term,Uppercase_Map" +
  "ping=uc,Unified_Ideograph=uideo,Uppercase=upper,Variation_Selector=vs,Vertical_Orientation" +
  "=vo,Word_Break=wb,XID_Continue=xidc,XID_Start=xids"

export const GENERAL_CATEGORY_VALUES =
  "Other=c,Cased_Letter=lc,Control=cc|cntrl,Format=cf,Close_Punctuation=pe,Unassigned=cn,Priv" +
  "ate_Use=co,Mark=combiningmark|m,Connector_Punctuation=pc,Surrogate=cs,Currency_Symbol=sc,D" +
  "ash_Punctuation=pd,Decimal_Number=digit|nd,Enclosing_Mark=me,Final_Punctuation=pf,Initial_" +
  "Punctuation=pi,Letter=l,Letter_Number=nl,Line_Separator=zl,Lowercase_Letter=ll,Modifier_Le" +
  "tter=lm,Other_Letter=lo,Titlecase_Letter=lt,Uppercase_Letter=lu,Math_Symbol=sm,Spacing_Mar" +
  "k=mc,Nonspacing_Mark=mn,Modifier_Symbol=sk,Number=n,Other_Number=no,Open_Punctuation=ps,Ot" +
  "her_Punctuation=po,Other_Symbol=so,Punctuation=p|punct,Paragraph_Separator=zp,Symbol=s,Sep" +
  "arator=z,Space_Separator=zs"

export const SCRIPT_VALUES =
  "Adlam=adlm,Caucasian_Albanian=aghb,Ahom,Anatolian_Hieroglyphs=hluw,Arabic=arab,Armenian=ar" +
  "mn,Imperial_Aramaic=armi,Avestan=avst,Balinese=bali,Bamum=bamu,Bassa_Vah=bass,Batak=batk,B" +
  "engali=beng,Bhaiksuki=bhks,Bopomofo=bopo,Brahmi=brah,Braille=brai,Buginese=bugi,Buhid=buhd" +
  ",Chakma=cakm,Canadian_Aboriginal=cans,Carian=cari,Cham,Cherokee=cher,Chorasmian=chrs,Commo" +
  "n=zyyy,Coptic=copt|qaac,Cypro_Minoan=cpmn,Cypriot=cprt,Cuneiform=xsux,Cyrillic=cyrl,Desere" +
  "t=dsrt,Devanagari=deva,Dives_Akuru=diak,Dogra=dogr,Duployan=dupl,Egyptian_Hieroglyphs=egyp" +
  ",Elbasan=elba,Elymaic=elym,Ethiopic=ethi,Garay=gara,Georgian=geor,Glagolitic=glag,Gunjala_" +
  "Gondi=gong,Masaram_Gondi=gonm,Gothic=goth,Grantha=gran,Greek=grek,Gujarati=gujr,Gurung_Khe" +
  "ma=gukh,Gurmukhi=guru,Han=hani,Hangul=hang,Hanifi_Rohingya=rohg,Hanunoo=hano,Hatran=hatr,H" +
  "ebrew=hebr,Hiragana=hira,Pahawh_Hmong=hmng,Nyiakeng_Puachue_Hmong=hmnp,Katakana_Or_Hiragan" +
  "a=hrkt,Old_Hungarian=hung,Inherited=qaai|zinh,Inscriptional_Pahlavi=phli,Inscriptional_Par" +
  "thian=prti,Old_Italic=ital,Javanese=java,Kaithi=kthi,Kayah_Li=kali,Katakana=kana,Kannada=k" +
  "nda,Kawi,Kharoshthi=khar,Khitan_Small_Script=kits,Khmer=khmr,Khojki=khoj,Khudawadi=sind,Ki" +
  "rat_Rai=krai,Tai_Tham=lana,Lao=laoo,Latin=latn,Lepcha=lepc,Limbu=limb,Linear_A=lina,Linear" +
  "_B=linb,Lisu,Lycian=lyci,Lydian=lydi,Mahajani=mahj,Makasar=maka,Malayalam=mlym,Mandaic=man" +
  "d,Manichaean=mani,Marchen=marc,Medefaidrin=medf,Meetei_Mayek=mtei,Mende_Kikakui=mend,Meroi" +
  "tic_Cursive=merc,Meroitic_Hieroglyphs=mero,Miao=plrd,Modi,Mongolian=mong,Mro=mroo,Multani=" +
  "mult,Myanmar=mymr,Nabataean=nbat,Nag_Mundari=nagm,Nandinagari=nand,Old_North_Arabian=narb," +
  "Newa,New_Tai_Lue=talu,Nko=nkoo,Nushu=nshu,Ogham=ogam,Ol_Chiki=olck,Old_Permic=perm,Old_Per" +
  "sian=xpeo,Old_Sogdian=sogo,Old_South_Arabian=sarb,Old_Turkic=orkh,Old_Uyghur=ougr,Ol_Onal=" +
  "onao,Oriya=orya,Osage=osge,Osmanya=osma,Palmyrene=palm,Pau_Cin_Hau=pauc,Phags_Pa=phag,Psal" +
  "ter_Pahlavi=phlp,Phoenician=phnx,Rejang=rjng,Runic=runr,Samaritan=samr,Saurashtra=saur,Sig" +
  "nWriting=sgnw,Sharada=shrd,Shavian=shaw,Siddham=sidd,Sinhala=sinh,Sogdian=sogd,Sora_Sompen" +
  "g=sora,Soyombo=soyo,Sundanese=sund,Sunuwar=sunu,Syloti_Nagri=sylo,Syriac=syrc,Tagalog=tglg" +
  ",Tagbanwa=tagb,Tai_Le=tale,Tai_Viet=tavt,Takri=takr,Tamil=taml,Tangut=tang,Tangsa=tnsa,Tel" +
  "ugu=telu,Tifinagh=tfng,Thaana=thaa,Thai,Tibetan=tibt,Tirhuta=tirh,Todhri=todr,Toto,Tulu_Ti" +
  "galari=tutg,Ugaritic=ugar,Unknown=zzzz,Vai=vaii,Vithkuqi=vith,Wancho=wcho,Warang_Citi=wara" +
  ",Yezidi=yezi,Yi=yiii,Zanabazar_Square=zanb"

export const BINARY_PROPERTIES =
  "ASCII_Hex_Digit,Alphabetic,Bidi_Control,Bidi_Mirrored,Case_Ignorable,Cased,Changes_When_Ca" +
  "sefolded,Changes_When_Casemapped,Changes_When_Lowercased,Changes_When_Titlecased,Changes_W" +
  "hen_Uppercased,Dash,Default_Ignorable_Code_Point,Deprecated,Diacritic,Emoji,Emoji_Componen" +
  "t,Emoji_Modifier,Emoji_Modifier_Base,Emoji_Presentation,Extended_Pictographic,Extender,Gra" +
  "pheme_Base,Grapheme_Extend,Grapheme_Link,Hex_Digit,Hyphen,IDS_Binary_Operator,IDS_Trinary_" +
  "Operator,IDS_Unary_Operator,ID_Compat_Math_Continue,ID_Compat_Math_Start,ID_Continue,ID_St" +
  "art,Ideographic,InCB,Join_Control,Logical_Order_Exception,Lowercase,Math,Modifier_Combinin" +
  "g_Mark,Noncharacter_Code_Point,Other_Alphabetic,Other_Default_Ignorable_Code_Point,Other_G" +
  "rapheme_Extend,Other_ID_Continue,Other_ID_Start,Other_Lowercase,Other_Math,Other_Uppercase" +
  ",Pattern_Syntax,Pattern_White_Space,Prepended_Concatenation_Mark,Quotation_Mark,Radical,Re" +
  "gional_Indicator,Sentence_Terminal,Soft_Dotted,Terminal_Punctuation,Unified_Ideograph,Uppe" +
  "rcase,Variation_Selector,White_Space,XID_Continue,XID_Start"
