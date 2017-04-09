#!/usr/bin/env python3.6
import unittest

from ssh import AuthorizedKeys

AUTHORIZED_KEYS_1 = """\
ssh-rsa sdfsdfsd/dsdf+=sfsdfsd/sdfsdfsdjxkyR1JwTL6ekxWiUVPHAJh8VRs7Mcjgv5QKjrD47epePfafmBnaKJ7DNuSTQDxsRCSv9EZBO4qabjri+Umf/z+s6s1D3LZ2tYBvccl0sBu6sAUsWb5Z9tNfEKCS2uU+sdfsdfsdf/sdfsdfsdf user1@localhost
ssh-foo sdfsdfsd/dsdf+=sfsdfsd/sdfsdfsdjxkyR1JwTL6ekxWiUVPHAJh8VRs7Mcjgv5QKjrD47epePfafmBnaKJ7DNuSTQDxsRCSv9EZBO4qabjri+Umf/z+s6s1D3LZ2tYBvccl0sBu6sAUsWb5Z9tqsdfsdfsdfsdf/sdfsdfsdf
ssh-rsa sdfsdfsd/dsdf+=sfsdfsd/sdfsdfsdjxkyR1JwTL6ekxWiUVPHAJh8VRs7Mcjgv5QKjrD47epePfafmBnaKJ7DNuSTQDxsRCSv9EZBO4qabjri+Umf/z+s6s1D3LZ2tYBvccl0sBu6sAqsdfqsdfsdfuU+sdfsdfsdf/sdfsdfsdf user2@localhost.localdomain
"""
ONE_KEY = "ssh-rsa AAAAB3NFf++65QxAG9jQzaaA7oIbgq0rkwmLAx4OFIOJEfvP+XuaTSXDI2TuK556w== rsa-key-20170110"


class TestAuthorizedKeys(unittest.TestCase):

    def test_parse(self):
        keys_obj = AuthorizedKeys(AUTHORIZED_KEYS_1)
        self.assertEqual(keys_obj.count, 3)
        self.assertEqual(keys_obj.keys[(
            "ssh-rsa",
            "sdfsdfsd/dsdf+=sfsdfsd/sdfsdfsdjxkyR1JwTL6ekxWiUVPHAJh8VRs7Mcjgv5QKjrD47epePfafmBnaKJ7DNuSTQDxsRCSv9EZBO4qabjri+Umf/z+s6s1D3LZ2tYBvccl0sBu6sAUsWb5Z9tNfEKCS2uU+sdfsdfsdf/sdfsdfsdf"
        )].comment, "user1@localhost")
        self.assertIsNone(keys_obj.keys[(
            "ssh-foo",
            "sdfsdfsd/dsdf+=sfsdfsd/sdfsdfsdjxkyR1JwTL6ekxWiUVPHAJh8VRs7Mcjgv5QKjrD47epePfafmBnaKJ7DNuSTQDxsRCSv9EZBO4qabjri+Umf/z+s6s1D3LZ2tYBvccl0sBu6sAUsWb5Z9tqsdfsdfsdfsdf/sdfsdfsdf"
        )].comment)
        self.assertEqual(keys_obj.keys[(
            "ssh-rsa",
            "sdfsdfsd/dsdf+=sfsdfsd/sdfsdfsdjxkyR1JwTL6ekxWiUVPHAJh8VRs7Mcjgv5QKjrD47epePfafmBnaKJ7DNuSTQDxsRCSv9EZBO4qabjri+Umf/z+s6s1D3LZ2tYBvccl0sBu6sAqsdfqsdfsdfuU+sdfsdfsdf/sdfsdfsdf"
        )].comment, "user2@localhost.localdomain")

    def test_add_or_update_key(self):
        keys_obj = AuthorizedKeys(AUTHORIZED_KEYS_1)
        keys_obj.add_or_update(
            "ssh-foo",
            "sdfsdfsd/dsdf+=sfsdfsd/sdfsdfsdjxkyR1JwTL6ekxWiUVPHAJh8VRs7Mcjgv5QKjrD47epePfafmBnaKJ7DNuSTQDxsRCSv9EZBO4qabjri+Umf/z+s6s1D3LZ2tYBvccl0sBu6sAUsWb5Z9tqsdfsdfsdfsdf/sdfsdfsdf",
            "newcomment")
        self.assertEqual(keys_obj.count, 3)
        self.assertEqual(keys_obj.keys[(
            "ssh-foo",
            "sdfsdfsd/dsdf+=sfsdfsd/sdfsdfsdjxkyR1JwTL6ekxWiUVPHAJh8VRs7Mcjgv5QKjrD47epePfafmBnaKJ7DNuSTQDxsRCSv9EZBO4qabjri+Umf/z+s6s1D3LZ2tYBvccl0sBu6sAUsWb5Z9tqsdfsdfsdfsdf/sdfsdfsdf"
        )].comment, "newcomment")
        keys_obj.add_or_update(
            "ssh-foo",
            "ssdfsdfsdf",
            "newcomment")
        self.assertEqual(keys_obj.count, 4)
        self.assertEqual(keys_obj.keys[(
            "ssh-foo",
            "ssdfsdfsdf"
        )].comment, "newcomment")

    def test_has_rsa_key(self):
        keys_obj = AuthorizedKeys(AUTHORIZED_KEYS_1)
        self.assertFalse(keys_obj.has_rsa_key("ssdfsdfsdf"))
        keys_obj.add_or_update(
            "ssh-foo",
            "ssdfsdfsdf",
            "newcomment")
        self.assertEqual(keys_obj.count, 4)
        self.assertFalse(keys_obj.has_rsa_key("ssdfsdfsdf"))
        keys_obj.add_or_update(
            "ssh-rsa",
            "ssdfsdfsdf",
            "newcomment")
        self.assertEqual(keys_obj.count, 5)
        self.assertTrue(keys_obj.has_rsa_key("ssdfsdfsdf"))
        keys_obj.add_or_update_rsa(
            "sdfsdfsdfsdfsdffsdfsdfsdfsdfsdf",
            "newcoertertmment")
        self.assertTrue(keys_obj.has_rsa_key("sdfsdfsdfsdfsdffsdfsdfsdfsdfsdf"))
        self.assertEqual(keys_obj.count, 6)
        keys_obj.add_or_update(
            "ssh-rsa",
            "sdfsdfsdfsdfsdffsdfsdfsdfsdfsdf",
            "foo")
        self.assertEqual(keys_obj.count, 6)

    def test_add_from_line(self):
        keys_obj = AuthorizedKeys(AUTHORIZED_KEYS_1)
        self.assertEqual(keys_obj.count, 3)
        keys_obj.add_or_update_full(ONE_KEY)
        self.assertEqual(keys_obj.count, 4)
        self.assertTrue(keys_obj.has_rsa_key("AAAAB3NFf++65QxAG9jQzaaA7oIbgq0rkwmLAx4OFIOJEfvP+XuaTSXDI2TuK556w=="))
        keys_obj.add_or_update_full(ONE_KEY)
        self.assertEqual(keys_obj.count, 4)
        self.assertTrue(keys_obj.has_rsa_key("AAAAB3NFf++65QxAG9jQzaaA7oIbgq0rkwmLAx4OFIOJEfvP+XuaTSXDI2TuK556w=="))
        print(keys_obj.get_authorized_keys_str())





