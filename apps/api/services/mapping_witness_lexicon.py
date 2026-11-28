"""Small given-name / surname lexicons for the Mapping Witness's name-order check.

Only used to notice that the values headed for ``first_name`` look like
surnames *and* the values headed for ``last_name`` look like given names — the
two halves of a swap must both corroborate before anything is flagged. Names
that are common as both (Lee, James, Thomas, Jordan…) are left out of both
lists, so an ambiguous value counts for neither side.

Source: the most frequent given names and surnames in public US census and
Social Security name tables (facts, not authored test data). Lower-case ASCII;
the check folds accents before lookup.
"""

from __future__ import annotations

GIVEN_NAMES: frozenset[str] = frozenset(
    """
    aaron abigail adam alan albert alexander alice amanda amber amy andrea andrew
    angela anna anne anthony ashley barbara benjamin betty beverly brandon brenda
    brian brittany bruce carl carol carolyn catherine charles cheryl christina
    christine christopher cynthia daniel deborah debra denise dennis diana diane
    donald donna dorothy douglas dylan edward elizabeth emily emma eric ethan
    eugene evelyn frances frank gary george gloria grace gregory hannah harold
    heather helen henry isabella jack jacob jacqueline janet janice jason jean
    jeffrey jennifer jeremy jessica joan john jonathan jose joseph joshua joyce
    juan judith judy julia julie justin karen katherine kathleen kathryn kayla
    keith kelly kenneth kevin kimberly kyle larry laura lauren linda lisa logan
    louis luis madison margaret maria marie marilyn mark mary matthew megan
    melissa michael michelle nancy natalie nathan nicholas nicole noah olivia
    pamela patricia paul peter rachel ralph randy raymond rebecca richard robert
    roger ronald rose russell ruth ryan samantha samuel sandra sara sarah sean
    sharon shirley sophia stephanie stephen steven susan teresa terry theresa
    timothy tyler victoria vincent virginia walter wayne william zachary ann luis
    ana sofia lucia carlos miguel javier diego pablo hans klaus jurgen pierre
    jean-luc francois giulia marco luca chiara
    """.split()
)

SURNAMES: frozenset[str] = frozenset(
    """
    smith johnson williams brown jones garcia miller davis rodriguez martinez
    hernandez lopez gonzalez wilson anderson taylor moore jackson martin
    thompson white harris sanchez clark ramirez lewis robinson walker young
    allen king wright scott torres nguyen hill flores green adams nelson baker
    hall rivera campbell mitchell carter roberts gomez phillips evans turner
    diaz parker cruz edwards collins reyes stewart morris morales murphy cook
    rogers gutierrez ortiz morgan cooper peterson bailey reed kelly howard ramos
    kim cox ward richardson watson brooks chavez wood bennett gray mendoza ruiz
    hughes price alvarez castillo sanders patel myers long ross foster jimenez
    powell jenkins perry russell sullivan bell coleman butler henderson barnes
    gonzales fisher vasquez simmons romero jordan patterson alexander hamilton
    graham reynolds griffin wallace west cole hayes bryant herrera gibson ellis
    tran medina aguilar stevens murray ford castro marshall owens harrison
    fernandez mcdonald woods washington kennedy wells vargas henry chen freeman
    webb tucker guzman burns crawford olson simpson porter hunter gordon mendez
    silva shaw snyder mason dixon munoz hunt hicks holmes palmer wagner black
    robertson boyd rose stone salazar fox warren mills meyer rice schmidt muller
    schneider fischer weber becker dubois moreau rossi russo ferrari esposito
    bianchi gomez
    """.split()
)

#: Names common enough as both a given name and a surname to prove nothing.
_AMBIGUOUS = GIVEN_NAMES & SURNAMES
GIVEN_ONLY: frozenset[str] = GIVEN_NAMES - _AMBIGUOUS
SURNAME_ONLY: frozenset[str] = SURNAMES - _AMBIGUOUS
