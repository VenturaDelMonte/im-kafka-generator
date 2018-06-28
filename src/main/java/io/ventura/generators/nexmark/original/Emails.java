package io.ventura.generators.nexmark.original;


import java.nio.charset.Charset;

public class Emails {
    public static final String EMAILS[] = {
            "ab.ca", "ac.at", "ac.be", "ac.jp", "ac.kr", "ac.uk", "acm.org",
            "airmail.net", "arizona.edu", "ask.com", "att.com", "auc.dk", "auth.gr",
            "baylor.edu", "bell-labs.com", "bellatlantic.net", "berkeley.edu",
            "brandeis.edu", "broadquest.com", "brown.edu", "cabofalso.com", "cas.cz",
            "clarkson.edu", "clustra.com", "cmu.edu", "cnr.it", "co.in", "co.jp",
            "cohera.com", "columbia.edu", "compaq.com", "computer.org",
            "concentric.net", "conclusivestrategies.com", "concordia.ca",
            "cornell.edu", "crossgain.com", "csufresno.edu", "cti.gr", "cwi.nl",
            "cwru.edu", "dauphine.fr", "dec.com", "du.edu", "duke.edu",
            "earthlink.net", "edu.au", "edu.cn", "edu.hk", "edu.sg", "emc.com",
            "ernet.in", "evergreen.edu", "fernuni-hagen.de", "filelmaker.com",
            "filemaker.com", "forth.gr", "forwiss.de", "fsu.edu", "gatech.edu",
            "gmu.edu", "gte.com", "hitachi.com", "hp.com", "ibm.com", "imag.fr",
            "indiana.edu", "infomix.com", "informix.com", "inria.fr", "intersys.com",
            "itc.it", "labs.com", "lante.com", "lbl.gov", "lehner.net", "llnl.gov",
            "lri.fr", "lucent.com", "memphis.edu", "microsoft.com", "mit.edu",
            "mitre.org", "monmouth.edu", "msn.com", "msstate.edu", "ncr.com",
            "neu.edu", "newpaltz.edu", "njit.edu", "nodak.edu", "ntua.gr", "nwu.edu",
            "nyu.edu", "ogi.edu", "okcu.edu", "oracle.com", "ou.edu", "panasonic.com",
            "pi.it", "pitt.edu", "poly.edu", "poznan.pl", "prc.com", "propel.com",
            "purdue.edu", "rice.edu", "rpi.edu", "rutgers.edu", "rwth-aachen.de",
            "savera.com", "sbphrd.com", "sds.no", "sdsc.edu", "sfu.ca",
            "sleepycat.com", "smu.edu", "solidtech.com", "stanford.edu", "sun.com",
            "sunysb.edu", "sybase.com", "telcordia.com", "temple.edu", "toronto.edu",
            "tue.nl", "twsu.edu", "ualberta.ca", "ubs.com", "ucd.ie", "ucdavis.edu",
            "ucf.edu", "ucla.edu", "ucr.edu", "ucsb.edu", "ucsd.edu", "ufl.edu",
            "uga.edu", "uic.edu", "uiuc.edu", "ul.pt", "umass.edu", "umb.edu",
            "umd.edu", "umich.edu", "umkc.edu", "unbc.ca", "unf.edu",
            "uni-freiburg.de", "uni-mannheim.de", "uni-marburg.de", "uni-mb.si",
            "uni-muenchen.de", "uni-muenster.de", "uni-sb.de", "uni-trier.de",
            "unical.it", "unizh.ch", "unl.edu", "upenn.edu", "uqam.ca", "uregina.ca",
            "usa.net", "ust.hk", "uta.edu", "utexas.edu", "uu.se", "uwaterloo.ca",
            "uwindsor.ca", "uwo.ca", "verity.com", "versata.com", "washington.edu",
            "whizbang.com", "wisc.edu", "wpi.edu", "yahoo.com", "yorku.ca",
            "zambeel.com"
    };

    public static final int NUM_EMAILS = EMAILS.length;

    private static final Charset US_ASCII = Charset.forName("US-ASCII");

    public static final byte[][] EMAILS_16 = new byte[NUM_EMAILS][];
    static {
    	for (int i = 0; i < NUM_EMAILS; i++) {
    		EMAILS_16[i] = EMAILS[i].substring(0, Math.min(16, EMAILS[i].length())).getBytes(US_ASCII);
		}
	}
}
