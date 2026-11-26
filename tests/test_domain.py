import unittest

from pydantic import ValidationError

from queueboard.domain import TicketState, start_offset


class ContractTest(unittest.TestCase):
    def setUp(self):
        self.payload = {"ticket_id":"CASE-42", "version":1, "queue":"general", "status":"waiting"}

    def test_round_trip(self):
        event = TicketState(**self.payload)
        self.assertEqual(TicketState.model_validate_json(event.model_dump_json()), event)

    def test_invalid_snapshot(self):
        for change in ({"ticket_id":"invalid|id"}, {"version":0}, {"version":True},
                       {"version":"1"}, {"queue":"new|old"}, {"status":"unknown"}, {"note":"sample"}):
            with self.subTest(change=change), self.assertRaises(ValidationError):
                TicketState(**(self.payload | change))

    def test_resume_positions(self):
        for position in (3, 5, 8):
            self.assertEqual(start_offset(position, 3, 8), position)
        self.assertEqual(start_offset(None, 0, 10), 0)

    def test_truncated_or_recreated_log(self):
        for args in ((None, 4, 10), (2, 4, 10), (11, 0, 10)):
            with self.subTest(args=args), self.assertRaises(RuntimeError):
                start_offset(*args)
