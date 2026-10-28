"""Parser tests on small excerpts that mirror the real files' structure."""

from pathlib import Path

from app.models.sanctions import EntityType
from app.sources.sanctions_parsers import country_code, parse_eu, parse_ofac, parse_uk, parse_un

OFAC = """<?xml version="1.0" encoding="utf-8"?>
<sanctionsData xmlns="https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/ENHANCED_XML">
  <publicationInfo><dataAsOf>2026-10-21T00:00:00-04:00</dataAsOf></publicationInfo>
  <entities>
    <entity id="4238">
      <generalInfo><identityId>1663</identityId><entityType refId="602">Vessel</entityType></generalInfo>
      <sanctionsLists><sanctionsList refId="1550" id="1" datePublished="1989-01-05">SDN List</sanctionsList></sanctionsLists>
      <sanctionsPrograms><sanctionsProgram refId="202" id="2">CUBA</sanctionsProgram></sanctionsPrograms>
      <names><name id="1"><isPrimary>true</isPrimary><isLowQuality>false</isLowQuality>
        <translations><translation id="1"><isPrimary>true</isPrimary><script refId="1">Latin</script>
          <formattedFullName>EBANO</formattedFullName></translation></translations></name></names>
      <features><feature id="1"><type featureTypeId="3">Vessel Flag</type><value>Cuba</value></feature></features>
      <identityDocuments><identityDocument id="1"><type refId="1626">Vessel Registration Identification</type>
        <documentNumber>IMO 7406784</documentNumber></identityDocument></identityDocuments>
    </entity>
    <entity id="9">
      <generalInfo><entityType refId="600">Individual</entityType></generalInfo>
      <names>
        <name id="2"><isPrimary>true</isPrimary><isLowQuality>false</isLowQuality><translations>
          <translation id="2"><script>Latin</script><formattedFullName>LUKASHENKO, Alexander</formattedFullName></translation>
          <translation id="3"><script>Cyrillic</script><formattedFullName>ЛУКАШЕНКО, Александр</formattedFullName></translation>
        </translations></name>
        <name id="4"><isPrimary>false</isPrimary><isLowQuality>true</isLowQuality><translations>
          <translation id="5"><script>Latin</script><formattedFullName>LUKASHENKA, Alyaksandr</formattedFullName></translation>
        </translations></name>
      </names>
      <addresses><address id="1"><country refId="1">Belarus</country></address></addresses>
      <features><feature id="2"><type featureTypeId="8">Birthdate</type><value>30 Aug 1954</value></feature>
        <feature id="3"><type>Nationality Country</type><value>Belarus</value></feature></features>
    </entity>
  </entities>
</sanctionsData>"""

UN = """<?xml version="1.0" encoding="UTF-8"?>
<CONSOLIDATED_LIST dateGenerated="2026-10-27T23:00:04.067Z">
  <INDIVIDUALS><INDIVIDUAL><DATAID>6908001</DATAID><FIRST_NAME>MOHAMMAD</FIRST_NAME>
    <SECOND_NAME>HASSAN</SECOND_NAME><THIRD_NAME>AKHUND</THIRD_NAME><UN_LIST_TYPE>Al-Qaida</UN_LIST_TYPE>
    <LISTED_ON>2001-01-25</LISTED_ON><NAME_ORIGINAL_SCRIPT>محمد حسن آخوند</NAME_ORIGINAL_SCRIPT>
    <NATIONALITY><VALUE>Afghanistan</VALUE></NATIONALITY>
    <INDIVIDUAL_ALIAS><QUALITY>Low</QUALITY><ALIAS_NAME>Mullah Hassan</ALIAS_NAME></INDIVIDUAL_ALIAS>
    <INDIVIDUAL_DATE_OF_BIRTH><YEAR>1955</YEAR></INDIVIDUAL_DATE_OF_BIRTH>
  </INDIVIDUAL></INDIVIDUALS>
  <ENTITIES><ENTITY><DATAID>6908402</DATAID><FIRST_NAME>ADF</FIRST_NAME><UN_LIST_TYPE>DRC</UN_LIST_TYPE>
    <ENTITY_ALIAS><QUALITY>Good</QUALITY><ALIAS_NAME>Allied Democratic Forces</ALIAS_NAME></ENTITY_ALIAS>
    <ENTITY_ADDRESS><COUNTRY>Democratic Republic of the Congo</COUNTRY></ENTITY_ADDRESS>
  </ENTITY></ENTITIES>
</CONSOLIDATED_LIST>"""

EU = """<?xml version="1.0" encoding="UTF-8"?>
<export xmlns="http://eu.europa.ec/fpi/fsd/export" generationDate="2026-10-23T17:05:43.888+02:00">
  <sanctionEntity euReferenceNumber="EU.27.28" logicalId="13">
    <regulation programme="IRQ" publicationDate="2003-07-08"/>
    <subjectType code="person"/>
    <nameAlias wholeName="Saddam Hussein Al-Tikriti" strong="true"/>
    <nameAlias wholeName="Abu Ali" strong="false"/>
    <citizenship countryIso2Code="IQ"/>
    <birthdate birthdate="1937-04-28" year="1937"/>
  </sanctionEntity>
  <sanctionEntity logicalId="99">
    <regulation programme="RUS" publicationDate="2024-06-24"/>
    <subjectType code="enterprise"/>
    <nameAlias wholeName="NS CENTURY" strong="true"/>
    <identification identificationTypeCode="imo" number="9333450"/>
  </sanctionEntity>
</export>"""

UK = """<?xml version="1.0" encoding="utf-8"?>
<Designations><DateGenerated>27/10/2026</DateGenerated>
  <Designation><DateDesignated>03/10/2017</DateDesignated><UniqueID>DPR0075</UniqueID>
    <Names><Name><Name6>Petrel 8</Name6><NameType>Primary name</NameType></Name>
      <Name><Name6>Jal Vahini</Name6><NameType>Alias</NameType></Name></Names>
    <RegimeName>DPRK Regulations 2019</RegimeName><IndividualEntityShip>Ship</IndividualEntityShip>
    <ShipDetails><Ship><IMONumbers><IMONumber>IMO9562233</IMONumber></IMONumbers>
      <CurrentBelievedFlagOfShips><CurrentBelievedFlagOfShip>Comoros</CurrentBelievedFlagOfShip>
      </CurrentBelievedFlagOfShips></Ship></ShipDetails>
  </Designation>
  <Designation><DateDesignated>25/01/2001</DateDesignated><UniqueID>AFG0006</UniqueID>
    <Names><Name><Name1>MOHAMMAD</Name1><Name2>HASSAN</Name2><Name6>AKHUND</Name6>
      <NameType>Primary Name</NameType></Name></Names>
    <NonLatinNames><NonLatinName><NameNonLatinScript>محمد حسن آخوند</NameNonLatinScript></NonLatinName></NonLatinNames>
    <RegimeName>Afghanistan</RegimeName><IndividualEntityShip>Individual</IndividualEntityShip>
    <Addresses><Address><AddressCountry>Afghanistan</AddressCountry></Address></Addresses>
    <IndividualDetails><Individual><DOBs><DOB>dd/mm/1945</DOB></DOBs></Individual></IndividualDetails>
  </Designation>
</Designations>"""


def _write(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p


def test_ofac(tmp_path: Path) -> None:
    r = parse_ofac(_write(tmp_path, "o.xml", OFAC))
    assert r.published_at and r.published_at.year == 2026
    vessel, person = r.entries
    assert vessel.entity_type == EntityType.vessel
    assert vessel.imo_numbers == ["7406784"] and vessel.countries == ["CU"]
    assert vessel.programs == ["CUBA"] and vessel.listed_on == "1989-01-05"
    assert person.primary_name == "LUKASHENKO, Alexander"
    assert {n.script for n in person.names} == {"Latin", "Cyrillic"}
    assert [n.is_weak for n in person.names if "LUKASHENKA" in n.name] == [True]
    assert person.countries == ["BY"] and person.birth_dates == ["30 Aug 1954"]


def test_un(tmp_path: Path) -> None:
    r = parse_un(_write(tmp_path, "u.xml", UN))
    ind, ent = r.entries
    assert ind.primary_name == "MOHAMMAD HASSAN AKHUND" and ind.countries == ["AF"]
    assert any(n.is_weak and n.name == "Mullah Hassan" for n in ind.names)
    assert any(n.script == "Arabic" for n in ind.names)
    assert ent.entity_type == EntityType.entity and ent.countries == ["CD"]


def test_eu(tmp_path: Path) -> None:
    r = parse_eu(_write(tmp_path, "e.xml", EU))
    saddam, ship = r.entries
    assert saddam.entity_type == EntityType.individual and saddam.countries == ["IQ"]
    assert [n.is_weak for n in saddam.names] == [False, True]
    assert ship.entity_type == EntityType.vessel and ship.imo_numbers == ["9333450"]


def test_uk(tmp_path: Path) -> None:
    r = parse_uk(_write(tmp_path, "k.xml", UK))
    ship, person = r.entries
    assert ship.entity_type == EntityType.vessel and ship.imo_numbers == ["9562233"]
    assert ship.countries == ["KM"] and ship.listed_on == "2017-10-03"
    assert person.primary_name == "MOHAMMAD HASSAN AKHUND"
    assert person.birth_dates == ["dd/mm/1945"]


def test_country_code() -> None:
    assert country_code("Korea, North") == "KP"
    assert country_code("Iran") == "IR"
    assert country_code("Region: Crimea") == "UA"
    assert country_code("Narnia") is None
