import unittest


import sys; sys.path.append("../src")
try:
    import event
except:
    print "Unable to import 'event' module."


class TestOneEventPerMultiLine(unittest.TestCase):
    def testMostSimpleEvent(self):
        lines = ['john was the base guitarrist and vocalist', 
                 'george was the solo guitarrist', 
                 'ringo was the drummer']
        conf = {'events_conf' : [{'eventtype' : 'my-type',
                                  'regexps' : ['^.*guitarrist.*$']}],
                                  'consolidation_conf' : {'field': 'count'}}
        event = create_event(line, conf)
        expected_event = {'eventtype' : 'my-type', 'count': 2}
        self.assertDictEqual(expected_event, event)

    #def testMultipleRegexps


if __name__ == "__main__":
    unittest.main()
