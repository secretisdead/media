import uuid
import time
import re
from ipaddress import ip_address
from enum import Enum
from datetime import datetime, timezone
import random

from sqlalchemy import Table, Column, PrimaryKeyConstraint, LargeBinary
from sqlalchemy import Integer, Float, String, MetaData, ForeignKey, distinct
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, and_, or_

from statement_helper import sort_statement, paginate_statement, id_filter
from statement_helper import time_cutoff_filter, string_equal_filter
from statement_helper import string_not_equal_filter, string_like_filter
from statement_helper import bitwise_filter, int_cutoff_filter
from statement_helper import remote_origin_filter
from base64_url import base64_url_encode, base64_url_decode
from idcollection import IDCollection
from parse_id import parse_id, get_id_bytes, generate_or_parse_id

def parse_status(status):
	if isinstance(status, str):
		status = MediumStatus[status]
	elif isinstance(status, int):
		status = MediumStatus(status)
	elif not isinstance(status, MediumStatus):
		raise TypeError('Unable to convert to medium status')
	return status

def parse_protection(protection):
	if isinstance(protection, str):
		protection = MediumProtection[protection]
	elif isinstance(protection, int):
		protection = MediumProtection(protection)
	elif not isinstance(protection, MediumProtection):
		raise TypeError('Unable to convert to medium protection')
	return protection

def parse_searchability(searchability):
	if isinstance(searchability, str):
		searchability = MediumSearchability[searchability]
	elif isinstance(searchability, int):
		searchability = MediumSearchability()
	elif not isinstance(searchability, MediumSearchability):
		raise TypeError('Unable to convert to medium searchability')
	return searchability

class MediumStatus(Enum):
	FORBIDDEN = -2
	COPYRIGHT = -1
	ALLOWED = 1

	def __int__(self):
		return self.value

	def __str__(self):
		return self.name

class MediumProtection(Enum):
	NONE = 1
	GROUPS = 2
	PRIVATE = 3

	def __int__(self):
		return self.value

	def __str__(self):
		return self.name

class MediumSearchability(Enum):
	HIDDEN = 1
	GROUPS = 2
	PUBLIC = 3

	def __int__(self):
		return self.value

	def __str__(self):
		return self.name

class Medium:
	def __init__(
			self,
			id=None,
			upload_time=None,
			creation_time=None,
			touch_time=None,
			uploader_remote_origin='127.0.0.1',
			uploader_id='',
			owner_id='',
			status=MediumStatus.ALLOWED,
			protection=MediumProtection.NONE,
			searchability=MediumSearchability.HIDDEN,
			group_bits=0,
			mime='',
			size=0,
			data1=0,
			data2=0,
			data3=0,
			data4=0,
			data5=0,
			data6=0,
			focus=0.5,
			like_count=0,
			tags=[],
		):
		self.id, self.id_bytes = generate_or_parse_id(id)

		current_time = time.time()

		if None == upload_time:
			upload_time = current_time
		self.upload_time = int(upload_time)
		self.upload_datetime = datetime.fromtimestamp(
			self.upload_time,
			timezone.utc,
		)

		if None == creation_time:
			creation_time = current_time
		self.creation_time = int(creation_time)
		self.creation_datetime = datetime.fromtimestamp(
			self.creation_time,
			timezone.utc,
		)

		if None == touch_time:
			touch_time = current_time
		self.touch_time = int(touch_time)
		self.touch_datetime = datetime.fromtimestamp(
			self.touch_time,
			timezone.utc,
		)

		self.uploader_remote_origin = ip_address(uploader_remote_origin)

		self.uploader_id, self.uploader_id_bytes = parse_id(uploader_id)
		self.uploader = None

		self.owner_id, self.owner_id_bytes = parse_id(owner_id)
		self.owner = None

		self.status = parse_status(status)
		self.protection = parse_protection(protection)
		self.searchability = parse_searchability(searchability)

		if isinstance(group_bits, int):
			group_bits = group_bits.to_bytes(2, 'big')
		else:
			group_bits = bytes(group_bits)
		self.group_bits = group_bits

		self.mime = str(mime)
		self.size = int(size)
		self.data1 = int(data1)
		self.data2 = int(data2)
		self.data3 = int(data3)
		self.data4 = int(data4)
		self.data5 = int(data5)
		self.data6 = int(data6)
		self.focus = float(focus)

		if not like_count:
			like_count = 0
		self.like_count = int(like_count)

		self.tags = tags

class Tag:
	def __init__(
			self,
			medium_id='',
			tag='',
		):
		self.medium_id, self.medium_id_bytes = parse_id(medium_id)
		self.tag = str(tag)

class Like:
	def __init__(
			self,
			id=None,
			creation_time=None,
			medium_id='',
			user_id='',
		):
		self.id, self.id_bytes = generate_or_parse_id(id)

		if None == creation_time:
			creation_time = current_time = time.time()
		self.creation_time = int(creation_time)
		self.creation_datetime = datetime.fromtimestamp(
			self.creation_time,
			timezone.utc,
		)

		self.medium_id, self.medium_id_bytes = parse_id(medium_id)
		self.medium = None

		self.user_id, self.user_id_bytes = parse_id(user_id)
		self.user = None

class Media:
	def __init__(self, engine, db_prefix='', install=False):
		self.engine = engine
		self.engine_session = sessionmaker(bind=self.engine)()

		self.db_prefix = db_prefix

		self.tag_length = 16
		self.mime_length = 32

		metadata = MetaData()

		default_bytes = 0b0 * 16

		# media tables
		self.media = Table(
			self.db_prefix + 'media',
			metadata,
			Column(
				'id',
				LargeBinary(16),
				primary_key=True,
				default=default_bytes,
			),
			Column('upload_time', Integer, default=0),
			Column('creation_time', Integer, default=0),
			Column('touch_time', Integer, default=0),
			Column(
				'uploader_remote_origin',
				LargeBinary(16),
				default=ip_address(default_bytes).packed,
			),
			Column('uploader_id', LargeBinary(16), default=default_bytes),
			Column('owner_id', LargeBinary(16), default=default_bytes),
			Column('status', Integer, default=int(MediumStatus.ALLOWED)),
			Column('protection', Integer, default=int(MediumProtection.NONE)),
			Column(
				'searchability',
				Integer,
				default=int(MediumSearchability.HIDDEN),
			),
			Column('group_bits', Integer, default=0),
			Column('mime', String(self.mime_length), default=''),
			Column('size', Integer, default=0),
			Column('data1', Integer, default=0),
			Column('data2', Integer, default=0),
			Column('data3', Integer, default=0),
			Column('data4', Integer, default=0),
			Column('data5', Integer, default=0),
			Column('data6', Integer, default=0),
			Column('focus', Float, default=0.5),
		)

		# tags tables
		self.tags = Table(
			self.db_prefix + 'tags',
			metadata,
			Column('medium_id', None, ForeignKey(self.db_prefix + 'media.id')),
			Column('tag', String(self.tag_length), default=''),
			PrimaryKeyConstraint('medium_id', 'tag'),
		)

		# likes tables
		self.likes = Table(
			self.db_prefix + 'likes',
			metadata,
			Column(
				'id',
				LargeBinary(16),
				primary_key=True,
				default=default_bytes,
			),
			Column('creation_time', Integer, default=0),
			Column('medium_id', None, ForeignKey(self.db_prefix + 'media.id')),
			Column('user_id', LargeBinary(16), default=default_bytes),
		)

		self.connection = self.engine.connect()

		if install:
			table_exists = self.engine.dialect.has_table(
				self.engine,
				self.db_prefix + 'media'
			)
			if not table_exists:
				metadata.create_all(self.engine)

	def uninstall(self):
		for table in [
				self.media,
				self.tags,
			]:
			table.drop(self.engine)

	def generate_random_seed(self):
		seed_base64_url, seed_bytes = parse_id(
			bytes(random.getrandbits(8) for i in range(4))
		)
		return seed_base64_url

	# retrieve media
	def get_medium(self, id):
		media = self.search_media(filter={'ids': id})
		return media.get(id)

	def prepare_media_search_conditions(self, filter):
		conditions = []
		conditions += id_filter(filter, 'ids', self.media.c.id)
		conditions += remote_origin_filter(
			filter,
			'uploader_remote_origins',
			self.media.c.uploader_remote_origin,
		)
		conditions += time_cutoff_filter(
			filter,
			'uploaded',
			self.media.c.upload_time,
		)
		conditions += time_cutoff_filter(
			filter,
			'created',
			self.media.c.creation_time,
		)
		conditions += time_cutoff_filter(
			filter,
			'touched',
			self.media.c.touch_time,
		)
		conditions += id_filter(filter, 'uploader_ids', self.media.c.uploader_id)
		conditions += id_filter(filter, 'owner_ids', self.media.c.owner_id)
		for parse_property, filter_field, column in [
				(parse_status, 'statuses', self.media.c.status),
				(parse_protection, 'protections', self.media.c.protection),
				(
					parse_searchability,
					'searchabilities',
					self.media.c.searchability,
				),
			]:
			if 'with_' + filter_field in filter:
				if list is not type(filter['with_' + filter_field]):
					filter['with_' + filter_field] = [
						filter['with_' + filter_field],
					]
				block_conditions = []
				for property in filter['with_' + filter_field]:
					try:
						property = parse_property(property)
					except:
						pass
					else:
						block_conditions.append(column == int(property))
				if block_conditions:
					conditions.append(or_(*block_conditions))
				else:
					conditions.append(False)
			if 'without_' + filter_field in filter:
				if list is not type(filter['without_' + filter_field]):
					filter['without_' + filter_field] = [
						filter['without_' + filter_field],
					]
				block_conditions = []
				for property in filter['without_' + filter_field]:
					try:
						property = parse_property(property)
					except:
						pass
					else:
						block_conditions.append(column != int(property))
				if block_conditions:
					# without should and_ multiple conditions together
					conditions.append(and_(*block_conditions))
				else:
					conditions.append(False)
		conditions += bitwise_filter(
			filter,
			'group_bits',
			self.media.c.group_bits,
		)
		conditions += string_equal_filter(
			filter,
			'with_mimes',
			self.media.c.mime,
		)
		conditions += string_not_equal_filter(
			filter,
			'without_mimes',
			self.media.c.mime,
		)
		conditions += int_cutoff_filter(
			filter,
			'smaller_than',
			'larger_than',
			self.media.c.size,
		)
		for i in range(1, 7):
			data = 'data' + str(i)
			conditions += int_cutoff_filter(
				filter,
				data + '_less_than',
				data + '_more_than',
				getattr(self.media.c, data),
			)
		if 'portrait' in filter:
			if filter['portrait']:
				conditions += [
					self.media.c.data1 > 0,
					self.media.c.data2 > 0,
					self.media.c.data1 < self.media.c.data2,
				]
			else:
				conditions += [
					self.media.c.data1 >= self.media.c.data2,
				]
		if 'landscape' in filter:
			if filter['landscape']:
				conditions += [
					self.media.c.data1 > 0,
					self.media.c.data2 > 0,
					self.media.c.data1 > self.media.c.data2,
				]
			else:
				conditions += [
					self.media.c.data1 <= self.media.c.data2,
				]
		if (
				'with_tags' in filter
				or 'without_tags' in filter
				or 'with_tags_like' in filter
				or 'without_tags_like' in filter
			):
			with_tags_conditions = []
			without_tags_conditions = []
			if 'with_tags' in filter:
				if list is not type(filter['with_tags']):
					filter['with_tags'] = [filter['with_tags']]
				for tag in filter['with_tags']:
					with_tags_conditions.append(self.tags.c.tag == tag)
			if 'without_tags' in filter:
				if list is not type(filter['without_tags']):
					filter['without_tags'] = [filter['without_tags']]
				for tag in filter['without_tags']:
					without_tags_conditions.append(self.tags.c.tag == tag)
			if 'with_tags_like' in filter:
				if list is not type(filter['with_tags_like']):
					filter['with_tags_like'] = [filter['with_tags_like']]
				for tag in filter['with_tags_like']:
					with_tags_conditions.append(self.tags.c.tag.like(tag, escape='\\'))
			if 'without_tags_like' in filter:
				if list is not type(filter['without_tags_like']):
					filter['without_tags_like'] = [filter['without_tags_like']]
				for tag in filter['without_tags_like']:
					without_tags_conditions.append(self.tags.c.tag.like(tag, escape='\\'))
			if with_tags_conditions:
				for tag_condition in with_tags_conditions:
					tags_subquery = self.engine_session.query(
						self.tags.c.medium_id
					).filter(
						tag_condition
					).subquery()
					conditions.append(self.media.c.id.in_(tags_subquery))
			if without_tags_conditions:
				for tag_condition in without_tags_conditions:
					tags_subquery = self.engine_session.query(
						self.tags.c.medium_id
					).filter(
						tag_condition
					).subquery()
					conditions.append(self.media.c.id.notin_(tags_subquery))

		return conditions

	def prepare_media_search_statement(self, filter, conditions=None):
		if not conditions:
			conditions = self.prepare_media_search_conditions(filter)

		liked_by_user_id_bytes = None
		if 'liked_by_user' in filter:
			try:
				liked_by_user_id, liked_by_user_id_bytes = parse_id(
					filter['liked_by_user']
				)
			except:
				pass

		if liked_by_user_id_bytes:
			# include specific user aggregated like counts
			like_counts_subquery = self.engine_session.query(
				self.likes.c.medium_id, func.count(self.likes.c.id)
			).filter(
				self.likes.c.user_id == liked_by_user_id_bytes,
			).group_by(self.likes.c.medium_id).subquery()

			liked_by_user_subquery = self.engine_session.query(
				self.likes.c.medium_id
			).filter(
				self.likes.c.user_id == liked_by_user_id_bytes,
			).group_by(self.likes.c.medium_id).subquery()

			conditions.append(self.media.c.id.in_(liked_by_user_subquery))
		else:
			# include all aggregated like counts
			like_counts_subquery = self.engine_session.query(
				self.likes.c.medium_id, func.count(self.likes.c.id)
			).group_by(self.likes.c.medium_id).subquery()

		statement = self.media.join(
			like_counts_subquery,
			like_counts_subquery.c.medium_id == self.media.c.id,
			isouter=True,
		).select()

		medium_id, like_count_column = like_counts_subquery.c

		if conditions:
			statement = statement.where(and_(*conditions))

		return statement, like_count_column

	def count_media(self, filter={}):
		statement, like_count_column = self.prepare_media_search_statement(filter)
		statement = statement.with_only_columns([func.count(self.media.c.id)])
		return self.connection.execute(statement).fetchone()[0]

	def media_size(self, filter={}):
		statement, like_count_column = self.prepare_media_search_statement(filter)
		statement = statement.with_only_columns([func.sum(self.media.c.size)])
		return self.connection.execute(statement).fetchone()[0]

	def search_media(
			self,
			filter={},
			sort='',
			order='',
			page=0,
			perpage=None,
		):
		statement, like_count_column = self.prepare_media_search_statement(filter)

		dialect = self.engine_session.bind.dialect.name
		if 'random:' == sort[:7] and dialect in [
				'mysql',
				'mssql',
				'sqlite',
				'postgresql',
				#'oracle',
			]:
			if 'asc' != order:
				order = 'desc'
			seed = int.from_bytes(get_id_bytes(sort[7:]), 'big')
			if dialect in ['mysql', 'mssql']:
				statement = statement.order_by(func.rand(seed))
			elif dialect in ['sqlite']:
				random.seed(seed)
				pseudo_rand = '('
				for i in range(0, 8):
					start = random.randint(0, 31)
					pseudo_rand += (
						'substr(hex(id), '
							+ str(start)
							+ ', '
							+ str(start + 1)
							+ ') ||'
					)
				#pseudo_rand = pseudo_rand[:-3] + ') ' + order
				pseudo_rand = pseudo_rand[:-3] + ')'
				random.seed()
				statement = statement.order_by(pseudo_rand)
			elif dialect in ['postgresql']:
				statement = statement.order_by(func.random(seed))
			#TODO seeded pseudo random in oracle (and others?)
			#elif dialect in ['oracle']:
			#	statement = statement.order_by('dbms_random.value')
			# always additional sort by creation time and id
			statement = statement.order_by(
				getattr(self.media.c.creation_time, order)(),
			)
			statement = statement.order_by(getattr(self.media.c.id, order)())
		elif 'likes' == sort:
			statement = statement.order_by(getattr(like_count_column, order)())
			statement = statement.order_by(getattr(self.media.c.id, order)())
		else:
			statement = sort_statement(
				statement,
				self.media,
				sort,
				order,
				'creation_time',
				True,
				[
					'creation_time',
					'id',
				]
			)
		statement = paginate_statement(statement, page, perpage)

		result = self.connection.execute(statement).fetchall()

		media = IDCollection()
		for row in result:
			medium = Medium(
				id=row[self.media.c.id],
				upload_time=row[self.media.c.upload_time],
				creation_time=row[self.media.c.creation_time],
				touch_time=row[self.media.c.touch_time],
				uploader_remote_origin=row[self.media.c.uploader_remote_origin],
				uploader_id=row[self.media.c.uploader_id],
				owner_id=row[self.media.c.owner_id],
				status=MediumStatus(row[self.media.c.status]),
				protection=MediumProtection(row[self.media.c.protection]),
				searchability=MediumSearchability(row[self.media.c.searchability]),
				group_bits=row[self.media.c.group_bits],
				mime=row[self.media.c.mime],
				size=row[self.media.c.size],
				data1=row[self.media.c.data1],
				data2=row[self.media.c.data2],
				data3=row[self.media.c.data3],
				data4=row[self.media.c.data4],
				data5=row[self.media.c.data5],
				data6=row[self.media.c.data6],
				focus=row[self.media.c.focus],
				like_count=row[like_count_column],
			)
			media.add(medium)
		return media

	def get_mimes(self):
		statement = self.media.select().with_only_columns(
				[self.media.c.mime]
			).group_by(
				self.media.c.mime
			)
		result = self.connection.execute(statement).fetchall()
		mimes = []
		for row in result:
			mimes.append(row[self.media.c.mime])
		return mimes

	def capture_adjacent_media_from_result(
			self,
			medium,
			result,
			like_count_column,
		):
		prev_medium = None
		next_medium = None
		temp_prev_medium = None
		capture_next = False
		for row in result:
			if row[self.media.c.id] == medium.id_bytes:
				prev_medium = temp_prev_medium
				capture_next = True
				continue

			if capture_next:
				next_medium = Medium(
					id=row[self.media.c.id],
					upload_time=row[self.media.c.upload_time],
					creation_time=row[self.media.c.creation_time],
					touch_time=row[self.media.c.touch_time],
					uploader_remote_origin=row[self.media.c.uploader_remote_origin],
					uploader_id=row[self.media.c.uploader_id],
					owner_id=row[self.media.c.owner_id],
					status=MediumStatus(row[self.media.c.status]),
					protection=MediumProtection(row[self.media.c.protection]),
					searchability=MediumSearchability(row[self.media.c.searchability]),
					group_bits=row[self.media.c.group_bits],
					mime=row[self.media.c.mime],
					size=row[self.media.c.size],
					data1=row[self.media.c.data1],
					data2=row[self.media.c.data2],
					data3=row[self.media.c.data3],
					data4=row[self.media.c.data4],
					data5=row[self.media.c.data5],
					data6=row[self.media.c.data6],
					focus=row[self.media.c.focus],
					like_count=row[like_count_column],
				)
				break

			temp_prev_medium = Medium(
				id=row[self.media.c.id],
				upload_time=row[self.media.c.upload_time],
				creation_time=row[self.media.c.creation_time],
				touch_time=row[self.media.c.touch_time],
				uploader_remote_origin=row[self.media.c.uploader_remote_origin],
				uploader_id=row[self.media.c.uploader_id],
				owner_id=row[self.media.c.owner_id],
				status=MediumStatus(row[self.media.c.status]),
				protection=MediumProtection(row[self.media.c.protection]),
				searchability=MediumSearchability(row[self.media.c.searchability]),
				group_bits=row[self.media.c.group_bits],
				mime=row[self.media.c.mime],
				size=row[self.media.c.size],
				data1=row[self.media.c.data1],
				data2=row[self.media.c.data2],
				data3=row[self.media.c.data3],
				data4=row[self.media.c.data4],
				data5=row[self.media.c.data5],
				data6=row[self.media.c.data6],
				focus=row[self.media.c.focus],
				like_count=row[like_count_column],
			)

		return prev_medium, next_medium

	def get_adjacent_media(
			self,
			medium,
			filter={},
			sort='',
			order='',
			page=0,
			perpage=None,
		):
		conditions = self.prepare_media_search_conditions(filter)
		statement, like_count_column = self.prepare_media_search_statement(
			filter,
			conditions=conditions,
		)

		dialect = self.engine_session.bind.dialect.name
		# like the old persephone this will probably not be very efficient
		# for archives with lots of media
		if 'random:' == sort[:7] and dialect in [
				'mysql',
				'mssql',
				'sqlite',
				'postgresql',
				#'oracle',
			]:
			if 'asc' != order:
				order = 'desc'
			seed = int.from_bytes(get_id_bytes(sort[7:]), 'big')
			if dialect in ['mysql', 'mssql']:
				statement = statement.order_by(func.rand(seed))
			elif dialect in ['sqlite']:
				random.seed(seed)
				pseudo_rand = '('
				for i in range(0, 8):
					start = random.randint(0, 31)
					pseudo_rand += (
						'substr(hex(id), '
							+ str(start)
							+ ', '
							+ str(start + 1)
							+ ') ||'
					)
				pseudo_rand = pseudo_rand[:-3] + ') ' + order
				random.seed()
				statement = statement.order_by(pseudo_rand)
			elif dialect in ['postgresql']:
				statement = statement.order_by(func.random(seed))
			#TODO seeded pseudo random in oracle (and others?)
			#elif dialect in ['oracle']:
			#	statement = statement.order_by('dbms_random.value')
			# always additional sort by creation time and id
			statement = statement.order_by(
				getattr(self.media.c.creation_time, order)(),
			)
			statement = statement.order_by(getattr(self.media.c.id, order)())
			result = self.connection.execute(statement).fetchall()
			return self.capture_adjacent_media_from_result(
				medium,
				result,
				like_count_column,
			)
		else:
			# limit to numerical sorts
			if 'upload_time' == sort:
				sort_column = self.media.c.upload_time
				target = medium.upload_time
			elif 'touch_time' == sort:
				sort_column = self.media.c.touch_time
				target = medium.touch_time
			elif 'size' == sort:
				sort_column = self.media.c.size
				target = medium.size
			elif 'data' == sort[:4] and hasattr(medium, sort):
				sort_column = getattr(self.media.c, sort)
				target = getattr(medium, sort)
			elif 'likes' == sort:
				sort_column = like_count_column
				target = medium.like_count
			# default to creation time sort
			else:
				sort_column = self.media.c.creation_time
				target = medium.creation_time

		# get all media with the same sort value as target to capture next/prev
		conditions_eq = conditions[:]
		conditions_eq.append(sort_column == target)
		statement_eq, like_count_column = self.prepare_media_search_statement(
			filter,
			conditions_eq,
		)
		statement_eq = statement_eq.order_by(sort_column.asc())
		statement_eq = statement_eq.order_by(self.media.c.creation_time.asc())
		statement_eq = statement_eq.order_by(self.media.c.id.asc())
		result_eq = self.connection.execute(statement_eq).fetchall()
		prev_medium, next_medium = self.capture_adjacent_media_from_result(
			medium,
			result_eq,
			like_count_column,
		)

		# if previous still isn't captured get first medium with greatest sort value less than target
		if not prev_medium:
			conditions_prev = conditions[:]
			conditions_prev.append(sort_column < target)
			statement_prev, like_count_column = self.prepare_media_search_statement(
				filter,
				conditions_prev,
			)
			statement_prev = statement_prev.limit(1)
			statement_prev = statement_prev.order_by(sort_column.desc())
			statement_prev = statement_prev.order_by(self.media.c.creation_time.desc())
			statement_prev = statement_prev.order_by(self.media.c.id.desc())
			result_prev = self.connection.execute(statement_prev).fetchall()
			if result_prev:
				row = result_prev[0]
				prev_medium = Medium(
					id=row[self.media.c.id],
					upload_time=row[self.media.c.upload_time],
					creation_time=row[self.media.c.creation_time],
					touch_time=row[self.media.c.touch_time],
					uploader_remote_origin=row[self.media.c.uploader_remote_origin],
					uploader_id=row[self.media.c.uploader_id],
					owner_id=row[self.media.c.owner_id],
					status=MediumStatus(row[self.media.c.status]),
					protection=MediumProtection(row[self.media.c.protection]),
					searchability=MediumSearchability(row[self.media.c.searchability]),
					group_bits=row[self.media.c.group_bits],
					mime=row[self.media.c.mime],
					size=row[self.media.c.size],
					data1=row[self.media.c.data1],
					data2=row[self.media.c.data2],
					data3=row[self.media.c.data3],
					data4=row[self.media.c.data4],
					data5=row[self.media.c.data5],
					data6=row[self.media.c.data6],
					focus=row[self.media.c.focus],
					like_count=row[like_count_column],
				)

		# if next still isn't captured get first medium with lowest sort value greater than target
		if not next_medium:
			conditions_next = conditions[:]
			conditions_next.append(sort_column > target)
			statement_next, like_count_column = self.prepare_media_search_statement(
				filter,
				conditions_next,
			)
			statement_next = statement_next.limit(1)
			statement_next = statement_next.order_by(sort_column.asc())
			statement_next = statement_next.order_by(self.media.c.creation_time.asc())
			statement_next = statement_next.order_by(self.media.c.id.asc())
			result_next = self.connection.execute(statement_next).fetchall()
			if result_next:
				row = result_next[0]
				next_medium = Medium(
					id=row[self.media.c.id],
					upload_time=row[self.media.c.upload_time],
					creation_time=row[self.media.c.creation_time],
					touch_time=row[self.media.c.touch_time],
					uploader_remote_origin=row[self.media.c.uploader_remote_origin],
					uploader_id=row[self.media.c.uploader_id],
					owner_id=row[self.media.c.owner_id],
					status=MediumStatus(row[self.media.c.status]),
					protection=MediumProtection(row[self.media.c.protection]),
					searchability=MediumSearchability(row[self.media.c.searchability]),
					group_bits=row[self.media.c.group_bits],
					mime=row[self.media.c.mime],
					size=row[self.media.c.size],
					data1=row[self.media.c.data1],
					data2=row[self.media.c.data2],
					data3=row[self.media.c.data3],
					data4=row[self.media.c.data4],
					data5=row[self.media.c.data5],
					data6=row[self.media.c.data6],
					focus=row[self.media.c.focus],
					like_count=row[like_count_column],
				)

		return prev_medium, next_medium

	# manipulate media
	def create_medium(self, **kwargs):
		medium = Medium(**kwargs)
		# preflight check for existing id
		collision_medium = self.get_medium(medium.id_bytes)
		if collision_medium:
			self.collision_medium = medium
			raise ValueError('Medium ID collision')
		self.connection.execute(
			self.media.insert(),
			id=medium.id_bytes,
			upload_time=int(medium.upload_time),
			creation_time=int(medium.creation_time),
			touch_time=int(medium.touch_time),
			uploader_remote_origin=medium.uploader_remote_origin.packed,
			uploader_id=medium.uploader_id_bytes,
			owner_id=medium.owner_id_bytes,
			status=int(medium.status),
			protection=int(medium.protection),
			searchability=int(medium.searchability),
			group_bits=int.from_bytes(medium.group_bits, 'big'),
			mime=str(medium.mime),
			size=int(medium.size),
			data1=int(medium.data1),
			data2=int(medium.data2),
			data3=int(medium.data3),
			data4=int(medium.data4),
			data5=int(medium.data5),
			data6=int(medium.data6),
			focus=float(medium.focus),
		)
		return medium

	def update_medium(self, id, **kwargs):
		medium = Medium(id=id, **kwargs)
		updates = {}
		if 'upload_time' in kwargs:
			updates['upload_time'] = int(medium.upload_time)
		if 'creation_time' in kwargs:
			updates['creation_time'] = int(medium.creation_time)
		if 'touch_time' in kwargs:
			updates['touch_time'] = int(medium.touch_time)
		else:
			updates['touch_time'] = int(time.time())
		if 'uploader_remote_origin' in kwargs:
			updates['uploader_remote_origin'] = medium.uploader_remote_origin.packed
		if 'uploader_id' in kwargs:
			updates['uploader_id'] = medium.uploader_id_bytes
		if 'owner_id' in kwargs:
			updates['owner_id'] = medium.owner_id_bytes
		if 'status' in kwargs:
			updates['status'] = int(medium.status)
		if 'protection' in kwargs:
			updates['protection'] = int(medium.protection)
		if 'searchability' in kwargs:
			updates['searchability'] = int(medium.searchability)
		if 'group_bits' in kwargs:
			updates['group_bits'] = int.from_bytes(medium.group_bits, 'big')
		if 'mime' in kwargs:
			updates['mime'] = str(medium.mime)
		if 'size' in kwargs:
			updates['size'] = int(medium.size)
		for i in range(1, 7):
			data = 'data' + str(i)
			if data in kwargs:
				updates[data] = int(getattr(medium, data))
		if 'focus' in kwargs:
			updates['focus'] = float(medium.focus)
		if 0 == len(updates):
			return
		self.connection.execute(
			self.media.update().values(**updates).where(
				self.media.c.id == medium.id_bytes
			)
		)

	def delete_medium(self, id):
		id = get_id_bytes(id)
		self.connection.execute(
			self.media.delete().where(self.media.c.id == id)
		)
		self.connection.execute(
			self.tags.delete().where(self.tags.c.medium_id == id)
		)
		self.connection.execute(
			self.likes.delete().where(self.likes.c.medium_id == id)
		)

	# retrieve tags
	def prepare_tags_search_statement(self, filter):
		conditions = []
		conditions += id_filter(filter, 'medium_ids', self.tags.c.medium_id)
		conditions += string_like_filter(filter, 'tags', self.tags.c.tag)

		statement = self.tags.select()
		if conditions:
			statement = statement.where(and_(*conditions))
		return statement

	def count_tags(self, filter={}):
		statement = self.prepare_tags_search_statement(filter)
		statement = statement.with_only_columns([func.count(self.tags.c.media_id)])
		return self.connection.execute(statement).fetchone()[0]

	def search_tags(
			self,
			filter={},
			sort='',
			order='',
			page=0,
			perpage=None,
		):
		statement = self.prepare_tags_search_statement(filter)

		statement = sort_statement(
			statement,
			self.tags,
			sort,
			order,
			'tag',
			True,
			[
				'tag',
				'medium_id',
			]
		)
		statement = paginate_statement(statement, page, perpage)

		result = self.connection.execute(statement).fetchall()

		tags = []
		for row in result:
			medium_id, medium_id_bytes = parse_id(row[self.tags.c.medium_id])
			tags.append({
				'medium_id': medium_id,
				'medium_id_bytes': medium_id_bytes,
				'tag': row[self.tags.c.tag],
			})
		return tags

	def populate_media_tags(self, media):
		if IDCollection != type(media):
			medium = media
			media = IDCollection()
			media.add(medium)
		medium_ids = []
		for medium in media.values():
			medium_ids.append(medium.id_bytes)
		tags = self.search_tags(filter={'medium_ids': medium_ids})
		medium_ids_to_tags = {}
		for tag in tags:
			if tag['medium_id'] not in medium_ids_to_tags:
				medium_ids_to_tags[tag['medium_id']] = []
			medium_ids_to_tags[tag['medium_id']].append(tag['tag'])
		for medium in media.values():
			if medium.id in medium_ids_to_tags:
				medium.tags = sorted(medium_ids_to_tags[medium.id])
			else:
				medium.tags = []

	# manipulate tags
	def set_tags(self, medium_ids, tags):
		self.remove_tags(medium_ids)
		self.add_tags(medium_ids, tags)

	def add_tags(self, medium_ids, tags):
		if list != type(medium_ids):
			medium_ids = [medium_ids]
		if list != type(tags):
			tags = [tags]
		rows = []
		for medium_id in list(set(medium_ids)):
			medium_id, medium_id_bytes = parse_id(medium_id)
			for tag in list(set(tags)):
				if not tag:
					continue
				rows.append({
					'medium_id': medium_id_bytes,
					'tag': tag,
				})
		if not rows:
			return
		#TODO sqlite will probably choke if there are many tags for many media
		#TODO this needs to be split up for insertion in batches probably
		self.connection.execute(self.tags.insert().values(rows))

	def remove_tags(self, medium_ids, tags=[]):
		if list != type(medium_ids):
			medium_ids = [medium_ids]
		medium_id_conditions = []
		for medium_id in list(set(medium_ids)):
			medium_id, medium_id_bytes = parse_id(medium_id)
			medium_id_conditions.append(self.tags.c.medium_id == medium_id_bytes)
		if not tags:
			self.engine.execute(
				self.tags.delete().where(
					or_(*medium_id_conditions)
				)
			)
			return
		if list != type(tags):
			tags = [tags]
		tag_conditions = []
		for tag in list(set(tags)):
			if not tag:
				continue
			tag_conditions.append(self.tags.c.tag == tag)
		statement = self.tags.delete().where(
			and_(
				or_(*medium_id_conditions),
				or_(*tag_conditions),
			)
		)
		self.connection.execute(statement)

	def delete_tags(self, tags=[]):
		if list != type(tags):
			tags = [tags]
		tag_conditions = []
		for tag in tags:
			if not tag:
				continue
			tag_conditions.append(self.tags.c.tag == tag)
		statement = self.tags.delete().where(
			or_(*tag_conditions),
		)
		self.connection.execute(statement)

	# retrieve tag counts
	def prepare_tag_counts_search_conditions(self, filter):
		conditions = []
		conditions += string_like_filter(
			filter,
			'tags',
			self.tags.c.tag,
		)
		if (
				'with_statuses' in filter
				or 'without_statuses' in filter
				or 'with_searchabilities' in filter
				or 'without_searchabilities' in filter
				or 'with_protections' in filter
				or 'without_protections' in filter
			):
			with_media_conditions = []
			without_media_conditions = []
			for field, parse, column in [
					('statuses', parse_status, self.media.c.status),
					(
						'searchabilities',
						parse_searchability,
						self.media.c.searchability,
					),
					('protections', parse_protection, self.media.c.protection),
				]:
				if 'with_' + field in filter:
					if list is not type(filter['with_' + field]):
						filter['with_' + field] = [filter['with_' + field]]
					for value in filter['with_' + field]:
						try:
							value = parse(value)
						except:
							with_media_conditions.append(False)
						else:
							with_media_conditions.append(column == int(value))
				if 'without_' + field in filter:
					if list is not type(filter['without_' + field]):
						filter['without_' + field] = [filter['without_' + field]]
					for value in filter['without_' + field]:
						try:
							value = parse(value)
						except:
							pass
						else:
							without_media_conditions.append(column == int(value))
			if with_media_conditions:
				for media_condition in with_media_conditions:
					media_subquery = self.engine_session.query(
						self.media.c.id
					).filter(
						media_condition
					).subquery()
					conditions.append(self.tags.c.medium_id.in_(media_subquery))
			if without_media_conditions:
				for media_condition in without_media_conditions:
					media_subquery = self.engine_session.query(
						self.media.c.id
					).filter(
						media_condition
					).subquery()
					conditions.append(self.tags.c.medium_id.notin_(media_subquery))

		return conditions

	def prepare_tag_counts_search_statement(self, filter, conditions=[]):
		if not conditions:
			conditions = self.prepare_tag_counts_search_conditions(filter)
		statement = self.tags.select().with_only_columns(
			[
				self.tags.c.tag,
				func.count(self.tags.c.tag),
			]
		).group_by(self.tags.c.tag)

		tag, tag_count_column = statement.c

		if conditions:
			statement = statement.where(and_(*conditions))

		return statement, tag_count_column

	def count_unique_tags(self, filter):
		conditions = self.prepare_tag_counts_search_conditions(filter)
		statement = self.tags.select().with_only_columns(
			[func.count(distinct(self.tags.c.tag))]
		)
		if conditions:
			statement = statement.where(and_(*conditions))
		return self.connection.execute(statement).fetchone()[0]

	def search_tag_counts(
			self,filter={},
			sort='',
			order='',
			page=0,
			perpage=None,
		):
		statement, count_column = self.prepare_tag_counts_search_statement(
			filter
		)

		if 'count' == sort:
			statement = statement.order_by(getattr(count_column, order)())
			statement = statement.order_by(getattr(self.tags.c.tag, order)())
		else:
			statement = sort_statement(
				statement,
				self.tags,
				sort,
				order,
				'tag',
				True,
				[
					'tag',
				]
			)
		statement = paginate_statement(statement, page, perpage)

		result = self.connection.execute(statement).fetchall()

		tags = []
		for row in result:
			tag, count = row
			tags.append({
				'tag': tag,
				'count': count,
			})
		return tags

	# retrieve likes
	def get_like(self, id):
		likes = self.search_likes(filter={'ids': id})
		return likes.get(id)

	def prepare_likes_search_statement(self, filter):
		conditions = []
		conditions += id_filter(filter, 'ids', self.likes.c.id)
		conditions += time_cutoff_filter(
			filter,
			'created',
			self.likes.c.creation_time,
		)
		conditions += id_filter(filter, 'medium_ids', self.likes.c.medium_id)
		conditions += id_filter(filter, 'user_ids', self.likes.c.user_id)

		statement = self.likes.select()
		if conditions:
			statement = statement.where(and_(*conditions))
		return statement

	def count_likes(self, filter={}):
		statement = self.prepare_likes_search_statement(filter)
		statement = statement.with_only_columns([func.count(self.likes.c.id)])
		return self.connection.execute(statement).fetchone()[0]

	def search_likes(
			self,
			filter={},
			sort='',
			order='',
			page=0,
			perpage=None,
		):
		statement = self.prepare_likes_search_statement(filter)

		statement = sort_statement(
			statement,
			self.likes,
			sort,
			order,
			'creation_time',
			True,
			[
				'creation_time',
				'id',
			]
		)
		statement = paginate_statement(statement, page, perpage)

		result = self.connection.execute(statement).fetchall()

		likes = IDCollection()
		for row in result:
			like = Like(
				id=row[self.likes.c.id],
				creation_time=row[self.likes.c.creation_time],
				medium_id=row[self.likes.c.medium_id],
				user_id=row[self.likes.c.user_id],
			)
			likes.add(like)
		return likes

	# manipulate likes
	def create_like(self, medium_id, user_id):
		like = Like(medium_id=medium_id, user_id=user_id)
		# preflight check for existing id
		if self.get_like(like.id_bytes):
			raise ValueError('Like ID collision')
		self.connection.execute(
			self.likes.insert(),
			id=like.id_bytes,
			creation_time=int(like.creation_time),
			medium_id=like.medium_id_bytes,
			user_id=like.user_id_bytes,
		)
		return like

	def delete_like(self, id):
		id = get_id_bytes(id)
		self.connection.execute(
			self.likes.delete().where(self.likes.c.id == id)
		)

	def delete_user_likes(self, user_id):
		user_id = get_id_bytes(user_id)
		self.connection.execute(
			self.likes.delete().where(self.likes.c.user_id == user_id)
		)
