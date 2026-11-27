package wire

// The Unicode property, category and script names the reference regex engine
// accepts, copied from its tables so every SDK resolves one spelling to the
// same set. Each entry is a canonical name, optionally followed by "=" and its
// other normalized aliases separated by "|". The normalized canonical name is
// always an alias too.
//
// The sets themselves come from the Unicode tables this build ships, which
// may be a different Unicode version than the server was built with.
const (
	// regexPropertyNames are the property names, with their aliases.
	regexPropertyNames = "Age,ASCII_Hex_Digit=ahex,Alphabetic=alpha,Bidi_Class=bc,Bidi_Control=bidic,Bidi_Mirror" +
		"ed=bidim,Bidi_Mirroring_Glyph=bmg,Bidi_Paired_Bracket=bpb,Bidi_Paired_Bracket_Type=bpt" +
		",Block=blk,Canonical_Combining_Class=ccc,Cased,Case_Folding=cf,Case_Ignorable=ci,Compo" +
		"sition_Exclusion=ce,Changes_When_Casefolded=cwcf,Changes_When_Casemapped=cwcm,Changes_" +
		"When_Lowercased=cwl,Changes_When_NFKC_Casefolded=cwkcf,Changes_When_Titlecased=cwt,Cha" +
		"nges_When_Uppercased=cwu,kAccountingNumeric=cjkaccountingnumeric,kCompatibilityVariant" +
		"=cjkcompatibilityvariant,kIICore=cjkiicore,kIRG_GSource=cjkirggsource,kIRG_HSource=cjk" +
		"irghsource,kIRG_JSource=cjkirgjsource,kIRG_KPSource=cjkirgkpsource,kIRG_KSource=cjkirg" +
		"ksource,kIRG_MSource=cjkirgmsource,kIRG_SSource=cjkirgssource,kIRG_TSource=cjkirgtsour" +
		"ce,kIRG_UKSource=cjkirguksource,kIRG_USource=cjkirgusource,kIRG_VSource=cjkirgvsource," +
		"kOtherNumeric=cjkothernumeric,kPrimaryNumeric=cjkprimarynumeric,kRSUnicode=cjkrsunicod" +
		"e|unicoderadicalstroke|urs,Full_Composition_Exclusion=compex,Dash,Decomposition_Mappin" +
		"g=dm,Decomposition_Type=dt,Default_Ignorable_Code_Point=di,Deprecated=dep,Diacritic=di" +
		"a,East_Asian_Width=ea,Emoji_Modifier_Base=ebase,Emoji_Component=ecomp,Emoji_Modifier=e" +
		"mod,Emoji,Emoji_Presentation=epres,Equivalent_Unified_Ideograph=equideo,Expands_On_NFC" +
		"=xonfc,Expands_On_NFD=xonfd,Expands_On_NFKC=xonfkc,Expands_On_NFKD=xonfkd,Extender=ext" +
		",Extended_Pictographic=extpict,FC_NFKC_Closure=fcnfkc,General_Category=gc,Grapheme_Clu" +
		"ster_Break=gcb,Grapheme_Base=grbase,Grapheme_Extend=grext,Grapheme_Link=grlink,Hangul_" +
		"Syllable_Type=hst,Hex_Digit=hex,Hyphen,ID_Continue=idc,ID_Compat_Math_Continue,ID_Comp" +
		"at_Math_Start,Ideographic=ideo,ID_Start=ids,IDS_Binary_Operator=idsb,IDS_Trinary_Opera" +
		"tor=idst,IDS_Unary_Operator=idsu,Indic_Conjunct_Break=incb,Indic_Positional_Category=i" +
		"npc,Indic_Syllabic_Category=insc,ISO_Comment=isc,Jamo_Short_Name=jsn,Joining_Group=jg," +
		"Join_Control=joinc,Joining_Type=jt,kEH_Cat,kEH_Desc,kEH_HG,kEH_IFAO,kEH_JSesh,kEH_NoMi" +
		"rror,kEH_NoRotate,Line_Break=lb,Lowercase_Mapping=lc,Logical_Order_Exception=loe,Lower" +
		"case=lower,Math,Modifier_Combining_Mark=mcm,Name=na,Unicode_1_Name=na1,Name_Alias,Nonc" +
		"haracter_Code_Point=nchar,NFC_Quick_Check=nfcqc,NFD_Quick_Check=nfdqc,NFKC_Casefold=nf" +
		"kccf,NFKC_Quick_Check=nfkcqc,NFKC_Simple_Casefold=nfkcscf,NFKD_Quick_Check=nfkdqc,Nume" +
		"ric_Type=nt,Numeric_Value=nv,Other_Alphabetic=oalpha,Other_Default_Ignorable_Code_Poin" +
		"t=odi,Other_Grapheme_Extend=ogrext,Other_ID_Continue=oidc,Other_ID_Start=oids,Other_Lo" +
		"wercase=olower,Other_Math=omath,Other_Uppercase=oupper,Pattern_Syntax=patsyn,Pattern_W" +
		"hite_Space=patws,Prepended_Concatenation_Mark=pcm,Quotation_Mark=qmark,Radical,Regiona" +
		"l_Indicator=ri,Sentence_Break=sb,Script=sc,Simple_Case_Folding=scf|sfc,Script_Extensio" +
		"ns=scx,Soft_Dotted=sd,Sentence_Terminal=sterm,Simple_Lowercase_Mapping=slc,Simple_Titl" +
		"ecase_Mapping=stc,Simple_Uppercase_Mapping=suc,White_Space=space|wspace,Titlecase_Mapp" +
		"ing=tc,Terminal_Punctuation=term,Uppercase_Mapping=uc,Unified_Ideograph=uideo,Uppercas" +
		"e=upper,Variation_Selector=vs,Vertical_Orientation=vo,Word_Break=wb,XID_Continue=xidc," +
		"XID_Start=xids"

	// regexCategoryValues are the general category values.
	regexCategoryValues = "Other=c,Cased_Letter=lc,Control=cc|cntrl,Format=cf,Close_Punctuation=pe,Unassigned=cn," +
		"Private_Use=co,Mark=combiningmark|m,Connector_Punctuation=pc,Surrogate=cs,Currency_Sym" +
		"bol=sc,Dash_Punctuation=pd,Decimal_Number=digit|nd,Enclosing_Mark=me,Final_Punctuation" +
		"=pf,Initial_Punctuation=pi,Letter=l,Letter_Number=nl,Line_Separator=zl,Lowercase_Lette" +
		"r=ll,Modifier_Letter=lm,Other_Letter=lo,Titlecase_Letter=lt,Uppercase_Letter=lu,Math_S" +
		"ymbol=sm,Spacing_Mark=mc,Nonspacing_Mark=mn,Modifier_Symbol=sk,Number=n,Other_Number=n" +
		"o,Open_Punctuation=ps,Other_Punctuation=po,Other_Symbol=so,Punctuation=p|punct,Paragra" +
		"ph_Separator=zp,Symbol=s,Separator=z,Space_Separator=zs"

	// regexScriptValues are the script names, with their ISO codes.
	regexScriptValues = "Adlam=adlm,Caucasian_Albanian=aghb,Ahom,Anatolian_Hieroglyphs=hluw,Arabic=arab,Armenia" +
		"n=armn,Imperial_Aramaic=armi,Avestan=avst,Balinese=bali,Bamum=bamu,Bassa_Vah=bass,Bata" +
		"k=batk,Bengali=beng,Bhaiksuki=bhks,Bopomofo=bopo,Brahmi=brah,Braille=brai,Buginese=bug" +
		"i,Buhid=buhd,Chakma=cakm,Canadian_Aboriginal=cans,Carian=cari,Cham,Cherokee=cher,Chora" +
		"smian=chrs,Common=zyyy,Coptic=copt|qaac,Cypro_Minoan=cpmn,Cypriot=cprt,Cuneiform=xsux," +
		"Cyrillic=cyrl,Deseret=dsrt,Devanagari=deva,Dives_Akuru=diak,Dogra=dogr,Duployan=dupl,E" +
		"gyptian_Hieroglyphs=egyp,Elbasan=elba,Elymaic=elym,Ethiopic=ethi,Garay=gara,Georgian=g" +
		"eor,Glagolitic=glag,Gunjala_Gondi=gong,Masaram_Gondi=gonm,Gothic=goth,Grantha=gran,Gre" +
		"ek=grek,Gujarati=gujr,Gurung_Khema=gukh,Gurmukhi=guru,Han=hani,Hangul=hang,Hanifi_Rohi" +
		"ngya=rohg,Hanunoo=hano,Hatran=hatr,Hebrew=hebr,Hiragana=hira,Pahawh_Hmong=hmng,Nyiaken" +
		"g_Puachue_Hmong=hmnp,Katakana_Or_Hiragana=hrkt,Old_Hungarian=hung,Inherited=qaai|zinh," +
		"Inscriptional_Pahlavi=phli,Inscriptional_Parthian=prti,Old_Italic=ital,Javanese=java,K" +
		"aithi=kthi,Kayah_Li=kali,Katakana=kana,Kannada=knda,Kawi,Kharoshthi=khar,Khitan_Small_" +
		"Script=kits,Khmer=khmr,Khojki=khoj,Khudawadi=sind,Kirat_Rai=krai,Tai_Tham=lana,Lao=lao" +
		"o,Latin=latn,Lepcha=lepc,Limbu=limb,Linear_A=lina,Linear_B=linb,Lisu,Lycian=lyci,Lydia" +
		"n=lydi,Mahajani=mahj,Makasar=maka,Malayalam=mlym,Mandaic=mand,Manichaean=mani,Marchen=" +
		"marc,Medefaidrin=medf,Meetei_Mayek=mtei,Mende_Kikakui=mend,Meroitic_Cursive=merc,Meroi" +
		"tic_Hieroglyphs=mero,Miao=plrd,Modi,Mongolian=mong,Mro=mroo,Multani=mult,Myanmar=mymr," +
		"Nabataean=nbat,Nag_Mundari=nagm,Nandinagari=nand,Old_North_Arabian=narb,Newa,New_Tai_L" +
		"ue=talu,Nko=nkoo,Nushu=nshu,Ogham=ogam,Ol_Chiki=olck,Old_Permic=perm,Old_Persian=xpeo," +
		"Old_Sogdian=sogo,Old_South_Arabian=sarb,Old_Turkic=orkh,Old_Uyghur=ougr,Ol_Onal=onao,O" +
		"riya=orya,Osage=osge,Osmanya=osma,Palmyrene=palm,Pau_Cin_Hau=pauc,Phags_Pa=phag,Psalte" +
		"r_Pahlavi=phlp,Phoenician=phnx,Rejang=rjng,Runic=runr,Samaritan=samr,Saurashtra=saur,S" +
		"ignWriting=sgnw,Sharada=shrd,Shavian=shaw,Siddham=sidd,Sinhala=sinh,Sogdian=sogd,Sora_" +
		"Sompeng=sora,Soyombo=soyo,Sundanese=sund,Sunuwar=sunu,Syloti_Nagri=sylo,Syriac=syrc,Ta" +
		"galog=tglg,Tagbanwa=tagb,Tai_Le=tale,Tai_Viet=tavt,Takri=takr,Tamil=taml,Tangut=tang,T" +
		"angsa=tnsa,Telugu=telu,Tifinagh=tfng,Thaana=thaa,Thai,Tibetan=tibt,Tirhuta=tirh,Todhri" +
		"=todr,Toto,Tulu_Tigalari=tutg,Ugaritic=ugar,Unknown=zzzz,Vai=vaii,Vithkuqi=vith,Wancho" +
		"=wcho,Warang_Citi=wara,Yezidi=yezi,Yi=yiii,Zanabazar_Square=zanb"

	// regexBinaryProperties are the property names that name a set on their own.
	regexBinaryProperties = "ASCII_Hex_Digit,Alphabetic,Bidi_Control,Bidi_Mirrored,Case_Ignorable,Cased,Changes_Whe" +
		"n_Casefolded,Changes_When_Casemapped,Changes_When_Lowercased,Changes_When_Titlecased,C" +
		"hanges_When_Uppercased,Dash,Default_Ignorable_Code_Point,Deprecated,Diacritic,Emoji,Em" +
		"oji_Component,Emoji_Modifier,Emoji_Modifier_Base,Emoji_Presentation,Extended_Pictograp" +
		"hic,Extender,Grapheme_Base,Grapheme_Extend,Grapheme_Link,Hex_Digit,Hyphen,IDS_Binary_O" +
		"perator,IDS_Trinary_Operator,IDS_Unary_Operator,ID_Compat_Math_Continue,ID_Compat_Math" +
		"_Start,ID_Continue,ID_Start,Ideographic,InCB,Join_Control,Logical_Order_Exception,Lowe" +
		"rcase,Math,Modifier_Combining_Mark,Noncharacter_Code_Point,Other_Alphabetic,Other_Defa" +
		"ult_Ignorable_Code_Point,Other_Grapheme_Extend,Other_ID_Continue,Other_ID_Start,Other_" +
		"Lowercase,Other_Math,Other_Uppercase,Pattern_Syntax,Pattern_White_Space,Prepended_Conc" +
		"atenation_Mark,Quotation_Mark,Radical,Regional_Indicator,Sentence_Terminal,Soft_Dotted" +
		",Terminal_Punctuation,Unified_Ideograph,Uppercase,Variation_Selector,White_Space,XID_C" +
		"ontinue,XID_Start"
)
