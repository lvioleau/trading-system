//! The delivery-area vocabulary: [`EicCode`], every Nord Pool delivery area the connector
//! knows, identified by its ENTSO-E EIC code. A hardcoded, append-only copy of the reference
//! table owned by the nordpool-connector repo (`crates/marketdata/delivery_areas.csv`, itself
//! dumped from the archived `delivery_area` feed). A verbatim copy of that CSV is vendored
//! next to this crate and a unit test pins the enum to it — when Nord Pool adds an area,
//! re-vendor the CSV and append the variant (never remove or rename one: stored history
//! decodes through `from_eic`).
//!
//! Deliberately carries **no numeric delivery-area ids**: the trading system speaks EIC end to
//! end; the connector alone owns the id mapping at its PMD wire boundary.

/// A Nord Pool delivery area, identified by its ENTSO-E EIC code. Variants carry Nord Pool's
/// short area code verbatim behind an `AREA_` prefix.
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EicCode {
    /// DK1 — `10YDK-1--------W`.
    AREA_DK1,
    /// DK2 — `10YDK-2--------M`.
    AREA_DK2,
    /// FI — `10YFI-1--------U`.
    AREA_FI,
    /// FRE — `10YDOM-1001A084H`.
    AREA_FRE,
    /// EE — `10Y1001A1001A39I`.
    AREA_EE,
    /// LV — `10YLV-1001A00074`.
    AREA_LV,
    /// LT — `10YLT-1001A0008Q`.
    AREA_LT,
    /// NO1 — `10YNO-1--------2`.
    AREA_NO1,
    /// NO2 — `10YNO-2--------T`.
    AREA_NO2,
    /// NO3 — `10YNO-3--------J`.
    AREA_NO3,
    /// NO4 — `10YNO-4--------9`.
    AREA_NO4,
    /// NO5 — `10Y1001A1001A48H`.
    AREA_NO5,
    /// SE1 — `10Y1001A1001A44P`.
    AREA_SE1,
    /// SE2 — `10Y1001A1001A45N`.
    AREA_SE2,
    /// SE3 — `10Y1001A1001A46L`.
    AREA_SE3,
    /// SE4 — `10Y1001A1001A47J`.
    AREA_SE4,
    /// CNOR — `10Y1001A1001A70O`.
    AREA_CNOR,
    /// COAC — `10Y1001A1001A885`.
    AREA_COAC,
    /// CORS — `10Y1001A1001A893`.
    AREA_CORS,
    /// CSUD — `10Y1001A1001A71M`.
    AREA_CSUD,
    /// MALTA — `10Y1001A1001A877`.
    AREA_MALTA,
    /// NORD — `10Y1001A1001A73I`.
    AREA_NORD,
    /// SARD — `10Y1001A1001A74G`.
    AREA_SARD,
    /// SICI — `10Y1001A1001A75E`.
    AREA_SICI,
    /// SUD — `10Y1001A1001A788`.
    AREA_SUD,
    /// PT — `10YPT-REN------W`.
    AREA_PT,
    /// ES — `10YES-REE------0`.
    AREA_ES,
    /// MO — `10YMA-ONE------O`.
    AREA_MO,
    /// UK — `10Y1001A1001A57G`.
    AREA_UK,
    /// NL — `10YNL----------L`.
    AREA_NL,
    /// BE — `10YBE----------2`.
    AREA_BE,
    /// TBW — `10YDE-ENBW-----N`.
    AREA_TBW,
    /// AMP — `10YDE-RWENET---I`.
    AREA_AMP,
    /// TTG — `10YDE-EON------1`.
    AREA_TTG,
    /// 50Hz — `10YDE-VE-------2`.
    AREA_50HZ,
    /// FR — `10YFR-RTE------C`.
    AREA_FR,
    /// CH — `10YCH-SWISSGRIDZ`.
    AREA_CH,
    /// AT — `10YAT-APG------L`.
    AREA_AT,
    /// DK1A — `10YDK-1-------AA`.
    AREA_DK1A,
    /// NO1A — `10Y1001A1001A64J`.
    AREA_NO1A,
    /// HR — `10YHR-HEP------M`.
    AREA_HR,
    /// BG — `10YCA-BULGARIA-R`.
    AREA_BG,
    /// IE — `10Y1001A1001A59C`.
    AREA_IE,
    /// PL — `10YPL-AREA-----S`.
    AREA_PL,
    /// GB2 — `10Y1001A1001A58E`.
    AREA_GB2,
    /// ELE — `38YNPSLVEXP----Y`.
    AREA_ELE,
    /// ELI — `38YNPSLVIMP----5`.
    AREA_ELI,
    /// ERI — `38YNPSRUIMP----S`.
    AREA_ERI,
    /// GER — `10YCB-GERMANY--8`.
    AREA_GER,
    /// LBE — `10Y1001A1001A56I`.
    AREA_LBE,
    /// LBI — `10Y1001A1001A55K`.
    AREA_LBI,
    /// LRE — `43YLRE-------008`.
    AREA_LRE,
    /// LRI — `43YLRI--------04`.
    AREA_LRI,
    /// PLA — `10YDOM-PL-SE-LT2`.
    AREA_PLA,
    /// ELES — `10YSI-ELES-----O`.
    AREA_ELES,
    /// CEPS — `10YCZ-CEPS-----N`.
    AREA_CEPS,
    /// MAVIR — `10YHU-MAVIR----U`.
    AREA_MAVIR,
    /// TEL — `10YRO-TEL------P`.
    AREA_TEL,
    /// GE — `10Y1001A1001B012`.
    AREA_GE,
    /// GB1A — `11Y0-0000-0265-K`.
    AREA_GB1A,
    /// PLC — `10Y1001C--00038X`.
    AREA_PLC,
    /// ALBE — `22Y201903144---9`.
    AREA_ALBE,
    /// ALDE — `22Y201903145---4`.
    AREA_ALDE,
    /// ITCALA — `10Y1001C--00096J`.
    AREA_ITCALA,
    /// ITFR — `10Y1001A1001A81J`.
    AREA_ITFR,
    /// ITCH — `10Y1001A1001A68B`.
    AREA_ITCH,
    /// ITAT — `10Y1001A1001A80L`.
    AREA_ITAT,
    /// ITSI — `10Y1001A1001A67D`.
    AREA_ITSI,
    /// ITME — `10Y1001C--000611`.
    AREA_ITME,
    /// ITGR — `10Y1001A1001A66F`.
    AREA_ITGR,
    /// ITCP — `10YIT-GRTN-----B`.
    AREA_ITCP,
    /// DEA2 — `10YDOM-1001A082L`.
    AREA_DEA2,
    /// SK — `10YSK-SEPS-----K`.
    AREA_SK,
    /// GREE — `10YGR-HTSO-----Y`.
    AREA_GREE,
    /// NO2A — `10Y1001C--001219`.
    AREA_NO2A,
    /// LKAL — `10Y1001A1001A50U`.
    AREA_LKAL,
    /// DE-LU — `10Y1001A1001A82H`.
    AREA_DE_LU,
    /// BY — `10Y1001A1001A51S`.
    AREA_BY,
    /// RU — `10Y1001A1001A49F`.
    AREA_RU,
    /// GB — `10YGB----------A`.
    AREA_GB,
    /// NO2_SK — `50YCUY85S1HH29EK`.
    AREA_NO2_SK,
    /// DK1_SK — `45Y000000000001C`.
    AREA_DK1_SK,
    /// DK1_SB — `45Y0000000000038`.
    AREA_DK1_SB,
    /// DK2_SB — `45Y0000000000062`.
    AREA_DK2_SB,
    /// FI_FS — `44Y-00000000160K`.
    AREA_FI_FS,
    /// SE3_FS — `46Y000000000001Y`.
    AREA_SE3_FS,
    /// DK1_KS — `45Y000000000002A`.
    AREA_DK1_KS,
    /// SE3_KS — `46Y000000000002W`.
    AREA_SE3_KS,
    /// SE4_SP — `46Y000000000003U`.
    AREA_SE4_SP,
    /// SE4_NB — `46Y000000000004S`.
    AREA_SE4_NB,
    /// SE4_BC — `46Y000000000005Q`.
    AREA_SE4_BC,
    /// FI_EL — `44Y-00000000161I`.
    AREA_FI_EL,
    /// DK1_DE — `45Y0000000000054`.
    AREA_DK1_DE,
    /// DK2_KO — `45Y0000000000070`.
    AREA_DK2_KO,
    /// DK1_CO — `45Y0000000000046`.
    AREA_DK1_CO,
    /// NO2_ND — `50Y73EMZ34CQL9AJ`.
    AREA_NO2_ND,
    /// NO2_NK — `50YNBFFTWZRAHA3P`.
    AREA_NO2_NK,
    /// SE3_SWL — `46Y000000000017J`.
    AREA_SE3_SWL,
    /// SE4_SWL — `46Y000000000018H`.
    AREA_SE4_SWL,
    /// IS — `10Y1001A1001A958`.
    AREA_IS,
    /// UA-IPS — `10Y1001C--000182`.
    AREA_UA_IPS,
    /// LU — `10YLU-CEGEDEL-NQ`.
    AREA_LU,
    /// MD — `10Y1001A1001A990`.
    AREA_MD,
    /// RS — `10YCS-SERBIATSOV`.
    AREA_RS,
    /// UA-BEI — `10YUA-WEPS-----0`.
    AREA_UA_BEI,
    /// TR — `10YTR-TEIAS----W`.
    AREA_TR,
    /// MK — `10YMK-MEPSO----8`.
    AREA_MK,
    /// SE3A — `10Y1001C--00148Q`.
    AREA_SE3A,
    /// DE_DK1 — `10YDK1-DE-10010E`.
    AREA_DE_DK1,
    /// DE_KO — `10Y1001C--001510`.
    AREA_DE_KO,
    /// DE_NK — `10Y1001C--00152Z`.
    AREA_DE_NK,
    /// DE_BC — `10YDE-SE4-VBZ-01`.
    AREA_DE_BC,
    /// NL_CO — `49Y000000000003M`.
    AREA_NL_CO,
    /// NL_ND — `49Y000000000004K`.
    AREA_NL_ND,
    /// PL_LP — `10Y1001C--00153X`.
    AREA_PL_LP,
    /// PL_SP — `10Y1001C--00154V`.
    AREA_PL_SP,
    /// RO_BG — `10Y1001C--00158N`.
    AREA_RO_BG,
}

impl EicCode {
    /// Every known delivery area.
    pub const ALL: &'static [EicCode] = &[
        EicCode::AREA_DK1,
        EicCode::AREA_DK2,
        EicCode::AREA_FI,
        EicCode::AREA_FRE,
        EicCode::AREA_EE,
        EicCode::AREA_LV,
        EicCode::AREA_LT,
        EicCode::AREA_NO1,
        EicCode::AREA_NO2,
        EicCode::AREA_NO3,
        EicCode::AREA_NO4,
        EicCode::AREA_NO5,
        EicCode::AREA_SE1,
        EicCode::AREA_SE2,
        EicCode::AREA_SE3,
        EicCode::AREA_SE4,
        EicCode::AREA_CNOR,
        EicCode::AREA_COAC,
        EicCode::AREA_CORS,
        EicCode::AREA_CSUD,
        EicCode::AREA_MALTA,
        EicCode::AREA_NORD,
        EicCode::AREA_SARD,
        EicCode::AREA_SICI,
        EicCode::AREA_SUD,
        EicCode::AREA_PT,
        EicCode::AREA_ES,
        EicCode::AREA_MO,
        EicCode::AREA_UK,
        EicCode::AREA_NL,
        EicCode::AREA_BE,
        EicCode::AREA_TBW,
        EicCode::AREA_AMP,
        EicCode::AREA_TTG,
        EicCode::AREA_50HZ,
        EicCode::AREA_FR,
        EicCode::AREA_CH,
        EicCode::AREA_AT,
        EicCode::AREA_DK1A,
        EicCode::AREA_NO1A,
        EicCode::AREA_HR,
        EicCode::AREA_BG,
        EicCode::AREA_IE,
        EicCode::AREA_PL,
        EicCode::AREA_GB2,
        EicCode::AREA_ELE,
        EicCode::AREA_ELI,
        EicCode::AREA_ERI,
        EicCode::AREA_GER,
        EicCode::AREA_LBE,
        EicCode::AREA_LBI,
        EicCode::AREA_LRE,
        EicCode::AREA_LRI,
        EicCode::AREA_PLA,
        EicCode::AREA_ELES,
        EicCode::AREA_CEPS,
        EicCode::AREA_MAVIR,
        EicCode::AREA_TEL,
        EicCode::AREA_GE,
        EicCode::AREA_GB1A,
        EicCode::AREA_PLC,
        EicCode::AREA_ALBE,
        EicCode::AREA_ALDE,
        EicCode::AREA_ITCALA,
        EicCode::AREA_ITFR,
        EicCode::AREA_ITCH,
        EicCode::AREA_ITAT,
        EicCode::AREA_ITSI,
        EicCode::AREA_ITME,
        EicCode::AREA_ITGR,
        EicCode::AREA_ITCP,
        EicCode::AREA_DEA2,
        EicCode::AREA_SK,
        EicCode::AREA_GREE,
        EicCode::AREA_NO2A,
        EicCode::AREA_LKAL,
        EicCode::AREA_DE_LU,
        EicCode::AREA_BY,
        EicCode::AREA_RU,
        EicCode::AREA_GB,
        EicCode::AREA_NO2_SK,
        EicCode::AREA_DK1_SK,
        EicCode::AREA_DK1_SB,
        EicCode::AREA_DK2_SB,
        EicCode::AREA_FI_FS,
        EicCode::AREA_SE3_FS,
        EicCode::AREA_DK1_KS,
        EicCode::AREA_SE3_KS,
        EicCode::AREA_SE4_SP,
        EicCode::AREA_SE4_NB,
        EicCode::AREA_SE4_BC,
        EicCode::AREA_FI_EL,
        EicCode::AREA_DK1_DE,
        EicCode::AREA_DK2_KO,
        EicCode::AREA_DK1_CO,
        EicCode::AREA_NO2_ND,
        EicCode::AREA_NO2_NK,
        EicCode::AREA_SE3_SWL,
        EicCode::AREA_SE4_SWL,
        EicCode::AREA_IS,
        EicCode::AREA_UA_IPS,
        EicCode::AREA_LU,
        EicCode::AREA_MD,
        EicCode::AREA_RS,
        EicCode::AREA_UA_BEI,
        EicCode::AREA_TR,
        EicCode::AREA_MK,
        EicCode::AREA_SE3A,
        EicCode::AREA_DE_DK1,
        EicCode::AREA_DE_KO,
        EicCode::AREA_DE_NK,
        EicCode::AREA_DE_BC,
        EicCode::AREA_NL_CO,
        EicCode::AREA_NL_ND,
        EicCode::AREA_PL_LP,
        EicCode::AREA_PL_SP,
        EicCode::AREA_RO_BG,
    ];

    /// The ENTSO-E EIC code, e.g. `10YFI-1--------U` — the wire, storage, and config encoding.
    pub const fn eic_code(self) -> &'static str {
        match self {
            EicCode::AREA_DK1 => "10YDK-1--------W",
            EicCode::AREA_DK2 => "10YDK-2--------M",
            EicCode::AREA_FI => "10YFI-1--------U",
            EicCode::AREA_FRE => "10YDOM-1001A084H",
            EicCode::AREA_EE => "10Y1001A1001A39I",
            EicCode::AREA_LV => "10YLV-1001A00074",
            EicCode::AREA_LT => "10YLT-1001A0008Q",
            EicCode::AREA_NO1 => "10YNO-1--------2",
            EicCode::AREA_NO2 => "10YNO-2--------T",
            EicCode::AREA_NO3 => "10YNO-3--------J",
            EicCode::AREA_NO4 => "10YNO-4--------9",
            EicCode::AREA_NO5 => "10Y1001A1001A48H",
            EicCode::AREA_SE1 => "10Y1001A1001A44P",
            EicCode::AREA_SE2 => "10Y1001A1001A45N",
            EicCode::AREA_SE3 => "10Y1001A1001A46L",
            EicCode::AREA_SE4 => "10Y1001A1001A47J",
            EicCode::AREA_CNOR => "10Y1001A1001A70O",
            EicCode::AREA_COAC => "10Y1001A1001A885",
            EicCode::AREA_CORS => "10Y1001A1001A893",
            EicCode::AREA_CSUD => "10Y1001A1001A71M",
            EicCode::AREA_MALTA => "10Y1001A1001A877",
            EicCode::AREA_NORD => "10Y1001A1001A73I",
            EicCode::AREA_SARD => "10Y1001A1001A74G",
            EicCode::AREA_SICI => "10Y1001A1001A75E",
            EicCode::AREA_SUD => "10Y1001A1001A788",
            EicCode::AREA_PT => "10YPT-REN------W",
            EicCode::AREA_ES => "10YES-REE------0",
            EicCode::AREA_MO => "10YMA-ONE------O",
            EicCode::AREA_UK => "10Y1001A1001A57G",
            EicCode::AREA_NL => "10YNL----------L",
            EicCode::AREA_BE => "10YBE----------2",
            EicCode::AREA_TBW => "10YDE-ENBW-----N",
            EicCode::AREA_AMP => "10YDE-RWENET---I",
            EicCode::AREA_TTG => "10YDE-EON------1",
            EicCode::AREA_50HZ => "10YDE-VE-------2",
            EicCode::AREA_FR => "10YFR-RTE------C",
            EicCode::AREA_CH => "10YCH-SWISSGRIDZ",
            EicCode::AREA_AT => "10YAT-APG------L",
            EicCode::AREA_DK1A => "10YDK-1-------AA",
            EicCode::AREA_NO1A => "10Y1001A1001A64J",
            EicCode::AREA_HR => "10YHR-HEP------M",
            EicCode::AREA_BG => "10YCA-BULGARIA-R",
            EicCode::AREA_IE => "10Y1001A1001A59C",
            EicCode::AREA_PL => "10YPL-AREA-----S",
            EicCode::AREA_GB2 => "10Y1001A1001A58E",
            EicCode::AREA_ELE => "38YNPSLVEXP----Y",
            EicCode::AREA_ELI => "38YNPSLVIMP----5",
            EicCode::AREA_ERI => "38YNPSRUIMP----S",
            EicCode::AREA_GER => "10YCB-GERMANY--8",
            EicCode::AREA_LBE => "10Y1001A1001A56I",
            EicCode::AREA_LBI => "10Y1001A1001A55K",
            EicCode::AREA_LRE => "43YLRE-------008",
            EicCode::AREA_LRI => "43YLRI--------04",
            EicCode::AREA_PLA => "10YDOM-PL-SE-LT2",
            EicCode::AREA_ELES => "10YSI-ELES-----O",
            EicCode::AREA_CEPS => "10YCZ-CEPS-----N",
            EicCode::AREA_MAVIR => "10YHU-MAVIR----U",
            EicCode::AREA_TEL => "10YRO-TEL------P",
            EicCode::AREA_GE => "10Y1001A1001B012",
            EicCode::AREA_GB1A => "11Y0-0000-0265-K",
            EicCode::AREA_PLC => "10Y1001C--00038X",
            EicCode::AREA_ALBE => "22Y201903144---9",
            EicCode::AREA_ALDE => "22Y201903145---4",
            EicCode::AREA_ITCALA => "10Y1001C--00096J",
            EicCode::AREA_ITFR => "10Y1001A1001A81J",
            EicCode::AREA_ITCH => "10Y1001A1001A68B",
            EicCode::AREA_ITAT => "10Y1001A1001A80L",
            EicCode::AREA_ITSI => "10Y1001A1001A67D",
            EicCode::AREA_ITME => "10Y1001C--000611",
            EicCode::AREA_ITGR => "10Y1001A1001A66F",
            EicCode::AREA_ITCP => "10YIT-GRTN-----B",
            EicCode::AREA_DEA2 => "10YDOM-1001A082L",
            EicCode::AREA_SK => "10YSK-SEPS-----K",
            EicCode::AREA_GREE => "10YGR-HTSO-----Y",
            EicCode::AREA_NO2A => "10Y1001C--001219",
            EicCode::AREA_LKAL => "10Y1001A1001A50U",
            EicCode::AREA_DE_LU => "10Y1001A1001A82H",
            EicCode::AREA_BY => "10Y1001A1001A51S",
            EicCode::AREA_RU => "10Y1001A1001A49F",
            EicCode::AREA_GB => "10YGB----------A",
            EicCode::AREA_NO2_SK => "50YCUY85S1HH29EK",
            EicCode::AREA_DK1_SK => "45Y000000000001C",
            EicCode::AREA_DK1_SB => "45Y0000000000038",
            EicCode::AREA_DK2_SB => "45Y0000000000062",
            EicCode::AREA_FI_FS => "44Y-00000000160K",
            EicCode::AREA_SE3_FS => "46Y000000000001Y",
            EicCode::AREA_DK1_KS => "45Y000000000002A",
            EicCode::AREA_SE3_KS => "46Y000000000002W",
            EicCode::AREA_SE4_SP => "46Y000000000003U",
            EicCode::AREA_SE4_NB => "46Y000000000004S",
            EicCode::AREA_SE4_BC => "46Y000000000005Q",
            EicCode::AREA_FI_EL => "44Y-00000000161I",
            EicCode::AREA_DK1_DE => "45Y0000000000054",
            EicCode::AREA_DK2_KO => "45Y0000000000070",
            EicCode::AREA_DK1_CO => "45Y0000000000046",
            EicCode::AREA_NO2_ND => "50Y73EMZ34CQL9AJ",
            EicCode::AREA_NO2_NK => "50YNBFFTWZRAHA3P",
            EicCode::AREA_SE3_SWL => "46Y000000000017J",
            EicCode::AREA_SE4_SWL => "46Y000000000018H",
            EicCode::AREA_IS => "10Y1001A1001A958",
            EicCode::AREA_UA_IPS => "10Y1001C--000182",
            EicCode::AREA_LU => "10YLU-CEGEDEL-NQ",
            EicCode::AREA_MD => "10Y1001A1001A990",
            EicCode::AREA_RS => "10YCS-SERBIATSOV",
            EicCode::AREA_UA_BEI => "10YUA-WEPS-----0",
            EicCode::AREA_TR => "10YTR-TEIAS----W",
            EicCode::AREA_MK => "10YMK-MEPSO----8",
            EicCode::AREA_SE3A => "10Y1001C--00148Q",
            EicCode::AREA_DE_DK1 => "10YDK1-DE-10010E",
            EicCode::AREA_DE_KO => "10Y1001C--001510",
            EicCode::AREA_DE_NK => "10Y1001C--00152Z",
            EicCode::AREA_DE_BC => "10YDE-SE4-VBZ-01",
            EicCode::AREA_NL_CO => "49Y000000000003M",
            EicCode::AREA_NL_ND => "49Y000000000004K",
            EicCode::AREA_PL_LP => "10Y1001C--00153X",
            EicCode::AREA_PL_SP => "10Y1001C--00154V",
            EicCode::AREA_RO_BG => "10Y1001C--00158N",
        }
    }

    /// Nord Pool's short human-readable area code, e.g. `FI` — for logs and labels.
    pub const fn area_code(self) -> &'static str {
        match self {
            EicCode::AREA_DK1 => "DK1",
            EicCode::AREA_DK2 => "DK2",
            EicCode::AREA_FI => "FI",
            EicCode::AREA_FRE => "FRE",
            EicCode::AREA_EE => "EE",
            EicCode::AREA_LV => "LV",
            EicCode::AREA_LT => "LT",
            EicCode::AREA_NO1 => "NO1",
            EicCode::AREA_NO2 => "NO2",
            EicCode::AREA_NO3 => "NO3",
            EicCode::AREA_NO4 => "NO4",
            EicCode::AREA_NO5 => "NO5",
            EicCode::AREA_SE1 => "SE1",
            EicCode::AREA_SE2 => "SE2",
            EicCode::AREA_SE3 => "SE3",
            EicCode::AREA_SE4 => "SE4",
            EicCode::AREA_CNOR => "CNOR",
            EicCode::AREA_COAC => "COAC",
            EicCode::AREA_CORS => "CORS",
            EicCode::AREA_CSUD => "CSUD",
            EicCode::AREA_MALTA => "MALTA",
            EicCode::AREA_NORD => "NORD",
            EicCode::AREA_SARD => "SARD",
            EicCode::AREA_SICI => "SICI",
            EicCode::AREA_SUD => "SUD",
            EicCode::AREA_PT => "PT",
            EicCode::AREA_ES => "ES",
            EicCode::AREA_MO => "MO",
            EicCode::AREA_UK => "UK",
            EicCode::AREA_NL => "NL",
            EicCode::AREA_BE => "BE",
            EicCode::AREA_TBW => "TBW",
            EicCode::AREA_AMP => "AMP",
            EicCode::AREA_TTG => "TTG",
            EicCode::AREA_50HZ => "50Hz",
            EicCode::AREA_FR => "FR",
            EicCode::AREA_CH => "CH",
            EicCode::AREA_AT => "AT",
            EicCode::AREA_DK1A => "DK1A",
            EicCode::AREA_NO1A => "NO1A",
            EicCode::AREA_HR => "HR",
            EicCode::AREA_BG => "BG",
            EicCode::AREA_IE => "IE",
            EicCode::AREA_PL => "PL",
            EicCode::AREA_GB2 => "GB2",
            EicCode::AREA_ELE => "ELE",
            EicCode::AREA_ELI => "ELI",
            EicCode::AREA_ERI => "ERI",
            EicCode::AREA_GER => "GER",
            EicCode::AREA_LBE => "LBE",
            EicCode::AREA_LBI => "LBI",
            EicCode::AREA_LRE => "LRE",
            EicCode::AREA_LRI => "LRI",
            EicCode::AREA_PLA => "PLA",
            EicCode::AREA_ELES => "ELES",
            EicCode::AREA_CEPS => "CEPS",
            EicCode::AREA_MAVIR => "MAVIR",
            EicCode::AREA_TEL => "TEL",
            EicCode::AREA_GE => "GE",
            EicCode::AREA_GB1A => "GB1A",
            EicCode::AREA_PLC => "PLC",
            EicCode::AREA_ALBE => "ALBE",
            EicCode::AREA_ALDE => "ALDE",
            EicCode::AREA_ITCALA => "ITCALA",
            EicCode::AREA_ITFR => "ITFR",
            EicCode::AREA_ITCH => "ITCH",
            EicCode::AREA_ITAT => "ITAT",
            EicCode::AREA_ITSI => "ITSI",
            EicCode::AREA_ITME => "ITME",
            EicCode::AREA_ITGR => "ITGR",
            EicCode::AREA_ITCP => "ITCP",
            EicCode::AREA_DEA2 => "DEA2",
            EicCode::AREA_SK => "SK",
            EicCode::AREA_GREE => "GREE",
            EicCode::AREA_NO2A => "NO2A",
            EicCode::AREA_LKAL => "LKAL",
            EicCode::AREA_DE_LU => "DE-LU",
            EicCode::AREA_BY => "BY",
            EicCode::AREA_RU => "RU",
            EicCode::AREA_GB => "GB",
            EicCode::AREA_NO2_SK => "NO2_SK",
            EicCode::AREA_DK1_SK => "DK1_SK",
            EicCode::AREA_DK1_SB => "DK1_SB",
            EicCode::AREA_DK2_SB => "DK2_SB",
            EicCode::AREA_FI_FS => "FI_FS",
            EicCode::AREA_SE3_FS => "SE3_FS",
            EicCode::AREA_DK1_KS => "DK1_KS",
            EicCode::AREA_SE3_KS => "SE3_KS",
            EicCode::AREA_SE4_SP => "SE4_SP",
            EicCode::AREA_SE4_NB => "SE4_NB",
            EicCode::AREA_SE4_BC => "SE4_BC",
            EicCode::AREA_FI_EL => "FI_EL",
            EicCode::AREA_DK1_DE => "DK1_DE",
            EicCode::AREA_DK2_KO => "DK2_KO",
            EicCode::AREA_DK1_CO => "DK1_CO",
            EicCode::AREA_NO2_ND => "NO2_ND",
            EicCode::AREA_NO2_NK => "NO2_NK",
            EicCode::AREA_SE3_SWL => "SE3_SWL",
            EicCode::AREA_SE4_SWL => "SE4_SWL",
            EicCode::AREA_IS => "IS",
            EicCode::AREA_UA_IPS => "UA-IPS",
            EicCode::AREA_LU => "LU",
            EicCode::AREA_MD => "MD",
            EicCode::AREA_RS => "RS",
            EicCode::AREA_UA_BEI => "UA-BEI",
            EicCode::AREA_TR => "TR",
            EicCode::AREA_MK => "MK",
            EicCode::AREA_SE3A => "SE3A",
            EicCode::AREA_DE_DK1 => "DE_DK1",
            EicCode::AREA_DE_KO => "DE_KO",
            EicCode::AREA_DE_NK => "DE_NK",
            EicCode::AREA_DE_BC => "DE_BC",
            EicCode::AREA_NL_CO => "NL_CO",
            EicCode::AREA_NL_ND => "NL_ND",
            EicCode::AREA_PL_LP => "PL_LP",
            EicCode::AREA_PL_SP => "PL_SP",
            EicCode::AREA_RO_BG => "RO_BG",
        }
    }

    /// Look up an area by its EIC code string.
    pub fn from_eic(eic: &str) -> Option<EicCode> {
        match eic {
            "10YDK-1--------W" => Some(EicCode::AREA_DK1),
            "10YDK-2--------M" => Some(EicCode::AREA_DK2),
            "10YFI-1--------U" => Some(EicCode::AREA_FI),
            "10YDOM-1001A084H" => Some(EicCode::AREA_FRE),
            "10Y1001A1001A39I" => Some(EicCode::AREA_EE),
            "10YLV-1001A00074" => Some(EicCode::AREA_LV),
            "10YLT-1001A0008Q" => Some(EicCode::AREA_LT),
            "10YNO-1--------2" => Some(EicCode::AREA_NO1),
            "10YNO-2--------T" => Some(EicCode::AREA_NO2),
            "10YNO-3--------J" => Some(EicCode::AREA_NO3),
            "10YNO-4--------9" => Some(EicCode::AREA_NO4),
            "10Y1001A1001A48H" => Some(EicCode::AREA_NO5),
            "10Y1001A1001A44P" => Some(EicCode::AREA_SE1),
            "10Y1001A1001A45N" => Some(EicCode::AREA_SE2),
            "10Y1001A1001A46L" => Some(EicCode::AREA_SE3),
            "10Y1001A1001A47J" => Some(EicCode::AREA_SE4),
            "10Y1001A1001A70O" => Some(EicCode::AREA_CNOR),
            "10Y1001A1001A885" => Some(EicCode::AREA_COAC),
            "10Y1001A1001A893" => Some(EicCode::AREA_CORS),
            "10Y1001A1001A71M" => Some(EicCode::AREA_CSUD),
            "10Y1001A1001A877" => Some(EicCode::AREA_MALTA),
            "10Y1001A1001A73I" => Some(EicCode::AREA_NORD),
            "10Y1001A1001A74G" => Some(EicCode::AREA_SARD),
            "10Y1001A1001A75E" => Some(EicCode::AREA_SICI),
            "10Y1001A1001A788" => Some(EicCode::AREA_SUD),
            "10YPT-REN------W" => Some(EicCode::AREA_PT),
            "10YES-REE------0" => Some(EicCode::AREA_ES),
            "10YMA-ONE------O" => Some(EicCode::AREA_MO),
            "10Y1001A1001A57G" => Some(EicCode::AREA_UK),
            "10YNL----------L" => Some(EicCode::AREA_NL),
            "10YBE----------2" => Some(EicCode::AREA_BE),
            "10YDE-ENBW-----N" => Some(EicCode::AREA_TBW),
            "10YDE-RWENET---I" => Some(EicCode::AREA_AMP),
            "10YDE-EON------1" => Some(EicCode::AREA_TTG),
            "10YDE-VE-------2" => Some(EicCode::AREA_50HZ),
            "10YFR-RTE------C" => Some(EicCode::AREA_FR),
            "10YCH-SWISSGRIDZ" => Some(EicCode::AREA_CH),
            "10YAT-APG------L" => Some(EicCode::AREA_AT),
            "10YDK-1-------AA" => Some(EicCode::AREA_DK1A),
            "10Y1001A1001A64J" => Some(EicCode::AREA_NO1A),
            "10YHR-HEP------M" => Some(EicCode::AREA_HR),
            "10YCA-BULGARIA-R" => Some(EicCode::AREA_BG),
            "10Y1001A1001A59C" => Some(EicCode::AREA_IE),
            "10YPL-AREA-----S" => Some(EicCode::AREA_PL),
            "10Y1001A1001A58E" => Some(EicCode::AREA_GB2),
            "38YNPSLVEXP----Y" => Some(EicCode::AREA_ELE),
            "38YNPSLVIMP----5" => Some(EicCode::AREA_ELI),
            "38YNPSRUIMP----S" => Some(EicCode::AREA_ERI),
            "10YCB-GERMANY--8" => Some(EicCode::AREA_GER),
            "10Y1001A1001A56I" => Some(EicCode::AREA_LBE),
            "10Y1001A1001A55K" => Some(EicCode::AREA_LBI),
            "43YLRE-------008" => Some(EicCode::AREA_LRE),
            "43YLRI--------04" => Some(EicCode::AREA_LRI),
            "10YDOM-PL-SE-LT2" => Some(EicCode::AREA_PLA),
            "10YSI-ELES-----O" => Some(EicCode::AREA_ELES),
            "10YCZ-CEPS-----N" => Some(EicCode::AREA_CEPS),
            "10YHU-MAVIR----U" => Some(EicCode::AREA_MAVIR),
            "10YRO-TEL------P" => Some(EicCode::AREA_TEL),
            "10Y1001A1001B012" => Some(EicCode::AREA_GE),
            "11Y0-0000-0265-K" => Some(EicCode::AREA_GB1A),
            "10Y1001C--00038X" => Some(EicCode::AREA_PLC),
            "22Y201903144---9" => Some(EicCode::AREA_ALBE),
            "22Y201903145---4" => Some(EicCode::AREA_ALDE),
            "10Y1001C--00096J" => Some(EicCode::AREA_ITCALA),
            "10Y1001A1001A81J" => Some(EicCode::AREA_ITFR),
            "10Y1001A1001A68B" => Some(EicCode::AREA_ITCH),
            "10Y1001A1001A80L" => Some(EicCode::AREA_ITAT),
            "10Y1001A1001A67D" => Some(EicCode::AREA_ITSI),
            "10Y1001C--000611" => Some(EicCode::AREA_ITME),
            "10Y1001A1001A66F" => Some(EicCode::AREA_ITGR),
            "10YIT-GRTN-----B" => Some(EicCode::AREA_ITCP),
            "10YDOM-1001A082L" => Some(EicCode::AREA_DEA2),
            "10YSK-SEPS-----K" => Some(EicCode::AREA_SK),
            "10YGR-HTSO-----Y" => Some(EicCode::AREA_GREE),
            "10Y1001C--001219" => Some(EicCode::AREA_NO2A),
            "10Y1001A1001A50U" => Some(EicCode::AREA_LKAL),
            "10Y1001A1001A82H" => Some(EicCode::AREA_DE_LU),
            "10Y1001A1001A51S" => Some(EicCode::AREA_BY),
            "10Y1001A1001A49F" => Some(EicCode::AREA_RU),
            "10YGB----------A" => Some(EicCode::AREA_GB),
            "50YCUY85S1HH29EK" => Some(EicCode::AREA_NO2_SK),
            "45Y000000000001C" => Some(EicCode::AREA_DK1_SK),
            "45Y0000000000038" => Some(EicCode::AREA_DK1_SB),
            "45Y0000000000062" => Some(EicCode::AREA_DK2_SB),
            "44Y-00000000160K" => Some(EicCode::AREA_FI_FS),
            "46Y000000000001Y" => Some(EicCode::AREA_SE3_FS),
            "45Y000000000002A" => Some(EicCode::AREA_DK1_KS),
            "46Y000000000002W" => Some(EicCode::AREA_SE3_KS),
            "46Y000000000003U" => Some(EicCode::AREA_SE4_SP),
            "46Y000000000004S" => Some(EicCode::AREA_SE4_NB),
            "46Y000000000005Q" => Some(EicCode::AREA_SE4_BC),
            "44Y-00000000161I" => Some(EicCode::AREA_FI_EL),
            "45Y0000000000054" => Some(EicCode::AREA_DK1_DE),
            "45Y0000000000070" => Some(EicCode::AREA_DK2_KO),
            "45Y0000000000046" => Some(EicCode::AREA_DK1_CO),
            "50Y73EMZ34CQL9AJ" => Some(EicCode::AREA_NO2_ND),
            "50YNBFFTWZRAHA3P" => Some(EicCode::AREA_NO2_NK),
            "46Y000000000017J" => Some(EicCode::AREA_SE3_SWL),
            "46Y000000000018H" => Some(EicCode::AREA_SE4_SWL),
            "10Y1001A1001A958" => Some(EicCode::AREA_IS),
            "10Y1001C--000182" => Some(EicCode::AREA_UA_IPS),
            "10YLU-CEGEDEL-NQ" => Some(EicCode::AREA_LU),
            "10Y1001A1001A990" => Some(EicCode::AREA_MD),
            "10YCS-SERBIATSOV" => Some(EicCode::AREA_RS),
            "10YUA-WEPS-----0" => Some(EicCode::AREA_UA_BEI),
            "10YTR-TEIAS----W" => Some(EicCode::AREA_TR),
            "10YMK-MEPSO----8" => Some(EicCode::AREA_MK),
            "10Y1001C--00148Q" => Some(EicCode::AREA_SE3A),
            "10YDK1-DE-10010E" => Some(EicCode::AREA_DE_DK1),
            "10Y1001C--001510" => Some(EicCode::AREA_DE_KO),
            "10Y1001C--00152Z" => Some(EicCode::AREA_DE_NK),
            "10YDE-SE4-VBZ-01" => Some(EicCode::AREA_DE_BC),
            "49Y000000000003M" => Some(EicCode::AREA_NL_CO),
            "49Y000000000004K" => Some(EicCode::AREA_NL_ND),
            "10Y1001C--00153X" => Some(EicCode::AREA_PL_LP),
            "10Y1001C--00154V" => Some(EicCode::AREA_PL_SP),
            "10Y1001C--00158N" => Some(EicCode::AREA_RO_BG),
            _ => None,
        }
    }
}

impl std::fmt::Display for EicCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.eic_code())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::EicCode;

    #[test]
    fn known_area_maps_between_eic_and_area_code() {
        let fr = EicCode::from_eic("10YFR-RTE------C").unwrap();
        assert_eq!(fr, EicCode::AREA_FR);
        assert_eq!(fr.area_code(), "FR");
        assert_eq!(fr.to_string(), "10YFR-RTE------C");
    }

    #[test]
    fn unknown_and_empty_eics_resolve_to_none() {
        assert_eq!(EicCode::from_eic("10YNO-SUCH-AREA-"), None);
        assert_eq!(EicCode::from_eic(""), None);
    }

    /// Vendored verbatim from the nordpool-connector repo
    /// (`crates/marketdata/delivery_areas.csv`) — that repo is the single owner.
    const REFERENCE_CSV: &str = include_str!("../delivery_areas.csv");

    #[test]
    fn enum_matches_the_vendored_reference_csv_exactly() {
        // Pins this hand-maintained copy to the connector's table variant-for-variant, so
        // drift between the repos is a failing test, not a review hope. The CSV's leading
        // id column is skipped — the trading system deliberately carries no numeric area
        // ids — and both the CSV and `ALL` are ordered by that id.
        let rows: Vec<(&str, &str)> = REFERENCE_CSV
            .lines()
            .skip(1)
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let mut fields = line.split(',');
                let _id = fields.next().unwrap();
                let area_code = fields.next().unwrap();
                let eic = fields.next().unwrap();
                (area_code, eic)
            })
            .collect();
        assert_eq!(rows.len(), EicCode::ALL.len(), "row/variant count mismatch");
        for (&area, &(area_code, eic)) in EicCode::ALL.iter().zip(&rows) {
            assert_eq!(area.area_code(), area_code, "{area:?}");
            assert_eq!(area.eic_code(), eic, "{area:?}");
        }
    }

    #[test]
    fn all_areas_roundtrip_and_are_unique() {
        let mut eics = HashSet::new();
        for &area in EicCode::ALL {
            assert!(
                eics.insert(area.eic_code()),
                "duplicate eic {}",
                area.eic_code()
            );
            assert_eq!(EicCode::from_eic(area.eic_code()), Some(area));
        }
    }
}
