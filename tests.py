import sys
import unittest
import uuid
import time
from datetime import datetime, timezone

from ipaddress import ip_address
from sqlalchemy import create_engine

from testhelper import TestHelper, compare_base_attributes
from base64_url import base64_url_encode, base64_url_decode
from media import Media, MediumStatus, MediumProtection, MediumSearchability
from media import Medium, Tag, parse_id

db_url = ''

class TestMedia(TestHelper):
	def setUp(self):
		if db_url:
			engine = create_engine(db_url)
		else:
			engine = create_engine('sqlite:///:memory:')

		self.media = Media(
			engine,
			install=True,
			db_prefix=base64_url_encode(uuid.uuid4().bytes),
		)

	def tearDown(self):
		if db_url:
			self.media.uninstall()

	def assert_non_medium_raises(self, f):
		# any non-medium object should raise
		for invalid_medium in [
				'string',
				1,
				['list'],
				{'dict': 'ionary'},
			]:
			with self.assertRaises(Exception):
				f(invalid_medium)

	def test_parse_id(self):
		for invalid_input in [
				'contains non base64_url characters $%^~',
				['list'],
				{'dict': 'ionary'},
			]:
			with self.assertRaises(Exception):
				id, id_bytes = parse_id(invalid_input)
		expected_bytes = uuid.uuid4().bytes
		expected_string = base64_url_encode(expected_bytes)
		# from bytes
		id, id_bytes = parse_id(expected_bytes)
		self.assertEqual(id_bytes, expected_bytes)
		self.assertEqual(id, expected_string)
		# from string
		id, id_bytes = parse_id(expected_string)
		self.assertEqual(id, expected_string)
		self.assertEqual(id_bytes, expected_bytes)

	# class instantiation, create, get, and defaults
	def test_medium_class_create_get_and_defaults(self):
		self.class_create_get_and_defaults(
			Medium,
			self.media.create_medium,
			self.media.get_medium,
			{
				'uploader_remote_origin': ip_address('127.0.0.1'),
				'uploader_id': '',
				'owner_id': '',
				'status': MediumStatus.ALLOWED,
				'protection': MediumProtection.NONE,
				'searchability': MediumSearchability.HIDDEN,
				'group_bits': int(0).to_bytes(2, 'big'),
				'mime': '',
				'data1': 0,
				'data2': 0,
				'data3': 0,
				'data4': 0,
				'data5': 0,
				'data6': 0,
			},
		)

	#TODO assert properties that default to current time
	#TODO assert properties that default to uuid bytes

	# class instantiation and db object creation with properties
	# id properties
	def test_medium_id_property(self):
		self.id_property(Medium, self.media.create_medium, 'id')

	def test_medium_uploader_id_property(self):
		self.id_property(Medium, self.media.create_medium, 'uploader_id')

	def test_medium_owner_id_property(self):
		self.id_property(Medium, self.media.create_medium, 'owner_id')

	# int properties
	def test_medium_size_property(self):
		self.int_property(
			Medium,
			self.media.create_medium,
			'size',
		)

	def test_medium_data_properties(self):
		for i in range(1, 7):
			data = 'data' + str(i)
			self.int_property(
				Medium,
				self.media.create_medium,
				data,
			)

	# time properties
	def test_medium_upload_time_property(self):
		self.time_property(Medium, self.media.create_medium, 'upload')

	def test_medium_creation_time_property(self):
		self.time_property(Medium, self.media.create_medium, 'creation')

	def test_medium_touch_time_property(self):
		self.time_property(Medium, self.media.create_medium, 'touch')

	# string properties
	def test_medium_mime_property(self):
		self.string_property(
			Medium,
			self.media.create_medium,
			'mime',
		)

	# delete
	def test_delete_medium(self):
		self.delete(
			self.media.create_medium,
			self.media.get_medium,
			self.media.delete_medium,
		)

	# id collision
	def test_media_id_collision(self):
		self.id_collision(self.media.create_medium)

	# unfiltered count
	def test_count_media(self):
		self.count(
			self.media.create_medium,
			self.media.count_media,
			self.media.delete_medium,
		)

	# unfiltered search
	def test_search_media(self):
		self.search(
			self.media.create_medium,
			self.media.search_media,
			self.media.delete_medium,
		)

	# sort order and pagination
	def test_search_media_upload_time_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'upload_time',
			self.media.search_media,
		)

	def test_search_media_creation_time_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'creation_time',
			self.media.search_media,
		)

	def test_search_media_touch_time_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'touch_time',
			self.media.search_media,
		)

	def test_search_media_size_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'size',
			self.media.search_media,
		)

	def test_search_media_data1_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'data1',
			self.media.search_media,
		)

	def test_search_media_data2_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'data2',
			self.media.search_media,
		)

	def test_search_media_data3_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'data3',
			self.media.search_media,
		)

	def test_search_media_data4_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'data4',
			self.media.search_media,
		)

	def test_search_media_data5_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'data5',
			self.media.search_media,
		)

	def test_search_media_data6_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'data6',
			self.media.search_media,
		)

	def test_search_media_mime_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.media.create_medium,
			'mime',
			self.media.search_media,
			first_value='a',
			middle_value='b',
			last_value='c',
		)

	#TODO assert seeded random sorts

	# search by id
	def test_search_media_by_id(self):
		self.search_by_id(
			self.media.create_medium,
			'id',
			self.media.search_media,
			'ids',
		)

	def test_search_media_by_uploader_id(self):
		self.search_by_id(
			self.media.create_medium,
			'uploader_id',
			self.media.search_media,
			'uploader_ids',
		)

	def test_search_media_by_owner_id(self):
		self.search_by_id(
			self.media.create_medium,
			'owner_id',
			self.media.search_media,
			'owner_ids',
		)

	# search by time
	def search_media_by_upload_time(self):
		self.search_by_time(
			self.media.create_medium,
			'upload_time',
			self.medium.search_media,
			'uploaded',
		)

	def search_media_by_creation_time(self):
		self.search_by_time(
			self.media.create_medium,
			'creation_time',
			self.medium.search_media,
			'created',
		)

	def search_media_by_touch_time(self):
		self.search_by_time(
			self.media.create_medium,
			'touch_time',
			self.medium.search_media,
			'touched',
		)

	# search by string like
	#TODO tag_names_like
	#TODO search by string not like
	#TODO tag_names_not_like

	# search by string equal
	def test_search_media_by_with_mime(self):
		self.search_by_string_equal(
			self.media.create_medium,
			'mime',
			self.media.search_media,
			'with_mimes',
		)

	#TODO search by string not equal
	#TODO
	def test_search_media_by_without_mime(self):
		return
		self.search_by_string_not_equal(
			self.media.create_medium,
			'mime',
			self.media.search_media,
			'without_mimes',
		)

	# search by remote origin
	def test_search_media_by_uploader_remote_origin(self):
		self.search_by_remote_origin(
			self.media.create_medium,
			'uploader_remote_origin',
			self.media.search_media,
			'uploader_remote_origins',
		)

	# medium status enum
	def test_medium_status_enum(self):
		for medium_status, name, value in [
				(MediumStatus.FORBIDDEN, 'FORBIDDEN', -2),
				(MediumStatus.COPYRIGHT, 'COPYRIGHT', -1),
				(MediumStatus.ALLOWED, 'ALLOWED', 1),
			]:
			self.assertEqual(medium_status, MediumStatus[name])
			self.assertEqual(medium_status, MediumStatus(value))

		for invalid_name in [
				'FAKE_MEDIUM_STATUS',
				'NOT_ALLOWED',
			]:
			with self.assertRaises(KeyError):
				MediumStatus[invalid_name]

		for invalid_value in [-3, 2, 1000]:
			with self.assertRaises(ValueError):
				MediumStatus(invalid_value)

	# medium protection enum
	def test_medium_protection_enum(self):
		for medium_protection, name, value in [
				(MediumProtection.NONE, 'NONE', 1),
				(MediumProtection.GROUPS, 'GROUPS', 2),
				(MediumProtection.PRIVATE, 'PRIVATE', 3),
			]:
			self.assertEqual(medium_protection, MediumProtection[name])
			self.assertEqual(medium_protection, MediumProtection(value))

		for invalid_name in [
				'FAKE_MEDIUM_PROTECTION',
				'PUBLIC',
			]:
			with self.assertRaises(KeyError):
				MediumProtection[invalid_name]

		for invalid_value in [-3, 4, 1000]:
			with self.assertRaises(ValueError):
				MediumProtection(invalid_value)

	# medium searchability enum
	def test_medium_searchability_enum(self):
		for medium_searchability, name, value in [
				(MediumSearchability.HIDDEN, 'HIDDEN', 1),
				(MediumSearchability.GROUPS, 'GROUPS', 2),
				(MediumSearchability.PUBLIC, 'PUBLIC', 3),
			]:
			self.assertEqual(medium_searchability, MediumSearchability[name])
			self.assertEqual(medium_searchability, MediumSearchability(value))

		for invalid_name in [
				'FAKE_MEDIUM_SEARCHABILITY',
				'PRIVATE',
			]:
			with self.assertRaises(KeyError):
				MediumSearchability[invalid_name]

		for invalid_value in [-3, 4, 1000]:
			with self.assertRaises(ValueError):
				MediumSearchability(invalid_value)

	# medium
	def test_update_medium(self):
		# update_medium instantiates a Medium object so anything that raises in
		# test_medium_class_create_get_and_defaults should raise
		medium = self.media.create_medium()

		user1_id = base64_url_encode(uuid.uuid4().bytes)
		user2_id = base64_url_encode(uuid.uuid4().bytes)

		# update_medium can receive a base64_url string
		properties = {
			'upload_time': 1000000000,
			'creation_time': 1111111111,
			'touch_time': 1234567890,
			'uploader_remote_origin': ip_address('1.2.3.4'),
			'uploader_id': user1_id,
			'owner_id': user1_id,
			'status': MediumStatus.COPYRIGHT,
			'protection': MediumProtection.GROUPS,
			'searchability': MediumSearchability.PUBLIC,
			'group_bits': int(1).to_bytes(2, 'big'),
			'mime': 'test1',
			'size': 1000,
			'data1': 1,
			'data2': 2,
			'data3': 3,
			'data4': 4,
			'data5': 5,
			'data6': 6,
		}
		self.media.update_medium(medium.id, **properties)
		medium = self.media.get_medium(medium.id_bytes)
		for key, value in properties.items():
			self.assertEqual(getattr(medium, key), value)

		# update_medium can receive bytes-like
		properties = {
			'upload_time': 1999999999,
			'creation_time': 2222222222,
			'touch_time':    2345678901,
			'uploader_remote_origin': ip_address('2.3.4.5'),
			'uploader_id': user2_id,
			'owner_id': user2_id,
			'status': MediumStatus.FORBIDDEN,
			'protection': MediumProtection.PRIVATE,
			'searchability': MediumSearchability.PUBLIC,
			'group_bits': int(2).to_bytes(2, 'big'),
			'mime': 'test2',
			'size': 2000,
			'data1': 7,
			'data2': 8,
			'data3': 9,
			'data4': 10,
			'data5': 11,
			'data6': 12,
		}
		self.media.update_medium(medium.id_bytes, **properties)
		medium = self.media.get_medium(medium.id_bytes)
		for key, value in properties.items():
			self.assertEqual(getattr(medium, key), value)

		self.assert_invalid_id_raises(self.media.update_medium)

	def test_search_medium_by_status(self):
		medium_allowed = self.media.create_medium(
			status=MediumStatus.ALLOWED,
		)
		medium_copyright = self.media.create_medium(
			status=MediumStatus.COPYRIGHT,
		)
		medium_forbidden = self.media.create_medium(
			status=MediumStatus.FORBIDDEN,
		)

		# single status
		media = self.media.search_media(
			filter={'with_statuses': MediumStatus.ALLOWED}
		)
		self.assertTrue(medium_allowed in media)
		self.assertTrue(medium_copyright not in media)
		self.assertTrue(medium_forbidden not in media)

		media = self.media.search_media(
			filter={'with_statuses': MediumStatus.COPYRIGHT}
		)
		self.assertTrue(medium_allowed not in media)
		self.assertTrue(medium_copyright in media)
		self.assertTrue(medium_forbidden not in media)

		media = self.media.search_media(
			filter={'with_statuses': MediumStatus.FORBIDDEN}
		)
		self.assertTrue(medium_allowed not in media)
		self.assertTrue(medium_copyright not in media)
		self.assertTrue(medium_forbidden in media)

		# multiple statuses
		media = self.media.search_media(
			filter={
				'with_statuses': [
					MediumStatus.ALLOWED,
					MediumStatus.COPYRIGHT,
				]
			}
		)
		self.assertTrue(medium_allowed in media)
		self.assertTrue(medium_copyright in media)
		self.assertTrue(medium_forbidden not in media)

		# a search with only invalid statuses should return no results
		media = self.media.search_media(filter={'with_statuses': [2, 100]})
		self.assertEqual(0, len(media))
		# a search with at least one valid status should behave normally
		# ignoring any invalid statuses
		media = self.media.search_media(
			filter={'with_statuses': [MediumStatus.ALLOWED, 2]}
		)
		self.assertTrue(medium_allowed in media)
		self.assertTrue(medium_copyright not in media)
		self.assertTrue(medium_forbidden not in media)

		#TODO without_statuses
		pass

	def test_search_medium_by_protection(self):
		medium_none = self.media.create_medium(
			protection=MediumProtection.NONE,
		)
		medium_groups = self.media.create_medium(
			protection=MediumProtection.GROUPS,
		)
		medium_private = self.media.create_medium(
			protection=MediumProtection.PRIVATE,
		)

		# single
		media = self.media.search_media(
			filter={'with_protections': MediumProtection.NONE}
		)
		self.assertTrue(medium_none in media)
		self.assertTrue(medium_groups not in media)
		self.assertTrue(medium_private not in media)

		media = self.media.search_media(
			filter={'with_protections': MediumProtection.GROUPS}
		)
		self.assertTrue(medium_none not in media)
		self.assertTrue(medium_groups in media)
		self.assertTrue(medium_private not in media)

		media = self.media.search_media(
			filter={'with_protections': MediumProtection.PRIVATE}
		)
		self.assertTrue(medium_none not in media)
		self.assertTrue(medium_groups not in media)
		self.assertTrue(medium_private in media)

		# multiple
		media = self.media.search_media(
			filter={
				'with_protections': [
					MediumProtection.NONE,
					MediumProtection.GROUPS,
				]
			}
		)
		self.assertTrue(medium_none in media)
		self.assertTrue(medium_groups in media)
		self.assertTrue(medium_private not in media)

		# a search with only invalid protections should return no results
		media = self.media.search_media(filter={'with_protections': [4, 100]})
		self.assertEqual(0, len(media))
		# a search with at least one valid protection should behave normally
		# ignoring any invalid protections
		media = self.media.search_media(
			filter={'with_protections': [MediumProtection.NONE, 4]}
		)
		self.assertTrue(medium_none in media)
		self.assertTrue(medium_groups not in media)
		self.assertTrue(medium_private not in media)

		#TODO without_protections
		pass

	def test_search_medium_by_searchability(self):
		medium_hidden = self.media.create_medium(
			searchability=MediumSearchability.HIDDEN,
		)
		medium_groups = self.media.create_medium(
			searchability=MediumSearchability.GROUPS,
		)
		medium_public = self.media.create_medium(
			searchability=MediumSearchability.PUBLIC,
		)

		# single
		media = self.media.search_media(
			filter={'with_searchabilities': MediumSearchability.HIDDEN}
		)
		self.assertTrue(medium_hidden in media)
		self.assertTrue(medium_groups not in media)
		self.assertTrue(medium_public not in media)

		media = self.media.search_media(
			filter={'with_searchabilities': MediumSearchability.GROUPS}
		)
		self.assertTrue(medium_hidden not in media)
		self.assertTrue(medium_groups in media)
		self.assertTrue(medium_public not in media)

		media = self.media.search_media(
			filter={'with_searchabilities': MediumSearchability.PUBLIC}
		)
		self.assertTrue(medium_hidden not in media)
		self.assertTrue(medium_groups not in media)
		self.assertTrue(medium_public in media)

		# multiple
		media = self.media.search_media(
			filter={
				'with_searchabilities': [
					MediumSearchability.HIDDEN,
					MediumSearchability.GROUPS,
				]
			}
		)
		self.assertTrue(medium_hidden in media)
		self.assertTrue(medium_groups in media)
		self.assertTrue(medium_public not in media)

		# a search with only invalid searchabilities should return no results
		media = self.media.search_media(
			filter={'with_searchabilities': [4, 100]},
		)
		self.assertEqual(0, len(media))
		# a search with at least one valid searchability should behave normally
		# ignoring any invalid earchabilities
		media = self.media.search_media(
			filter={'with_searchabilities': [MediumSearchability.HIDDEN, 4]}
		)
		self.assertTrue(medium_hidden in media)
		self.assertTrue(medium_groups not in media)
		self.assertTrue(medium_public not in media)

		#TODO without_searchabilities
		pass


	# search by group bits
	def test_search_media_by_group_bits(self):
		self.search_by_group_bits(
			self.media.create_medium,
			self.media.search_media,
		)

	#TODO
	# anonymization
	def test_anonymize_user_media(self):
		return
		user = self.users.create_user(name='test', display='Test')
		self.users.create_invite(created_by_user_id=user.id)
		self.users.create_invite(redeemed_by_user_id=user.id)
		self.users.create_session(user_id=user.id)
		self.users.create_authentication(user_id=user.id)
		self.users.create_permission(user_id=user.id)
		self.users.create_auto_permission(user_id=user.id)

		count_methods_filter_fields = [
			(self.users.count_invites, 'created_by_user_ids'),
			(self.users.count_invites, 'redeemed_by_user_ids'),
			(self.users.count_sessions, 'user_ids'),
			(self.users.count_authentications, 'user_ids'),
			(self.users.count_permissions, 'user_ids'),
			(self.users.count_auto_permissions, 'user_ids'),
		]
		self.assertIsNotNone(self.users.get_user(user.id))
		for count, filter_field in count_methods_filter_fields:
			self.assertEqual(1, count(filter={filter_field: user.id}))

		self.users.anonymize_user(user.id)

		self.assertIsNone(self.users.get_user(user.id))
		for count, filter_field in count_methods_filter_fields:
			self.assertEqual(0, count(filter={filter_field: user.id}))

	#TODO
	def test_anonymize_media_uploader_origins(self):
		return
		origin1 = '1.2.3.4'
		expected_anonymized_origin1 = '1.2.0.0'
		session1 = self.users.create_session(remote_origin=origin1)

		origin2 = '2001:0db8:85a3:0000:0000:8a2e:0370:7334'
		expected_anonymized_origin2 = '2001:0db8:85a3:0000:0000:0000:0000:0000'
		session2 = self.users.create_session(remote_origin=origin2)

		sessions = self.users.search_sessions()
		self.users.anonymize_session_origins(sessions)

		anonymized_session1 = self.users.get_session(session1.id)
		anonymized_session2 = self.users.get_session(session2.id)

		self.assertEqual(
			expected_anonymized_origin1,
			anonymized_session1.remote_origin.exploded,
		)
		self.assertEqual(
			expected_anonymized_origin2,
			anonymized_session2.remote_origin.exploded,
		)

if __name__ == '__main__':
	if '--db' in sys.argv:
		index = sys.argv.index('--db')
		if len(sys.argv) - 1 <= index:
			print('missing db url, usage:')
			print(' --db "dialect://user:password@server"')
			quit()
		db_url = sys.argv[index + 1]
		print('using specified db: "' + db_url + '"')
		del sys.argv[index:]
	else:
		print('using sqlite:///:memory:')
	print(
		'use --db [url] to test with specified db url'
			+ ' (e.g. sqlite:///media_tests.db)'
	)
	unittest.main()
