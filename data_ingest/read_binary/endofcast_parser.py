import os
import sys
import csv
import struct
import argparse
import pandas as pd
from dataclasses import dataclass
from typing import List, Tuple, Optional
from datetime import datetime, timezone, timedelta
from pathlib import Path


LABVIEW_EPOCH = datetime(1904, 1, 1, tzinfo=timezone.utc)


def labview_to_iso(ts: float, tz_offset_hours: int = 0) -> str:
	return (LABVIEW_EPOCH + timedelta(seconds=ts) + timedelta(hours=tz_offset_hours)).isoformat()


def labview_to_datetime(ts: float, tz_offset_hours: int = 0) -> datetime:
	"""Convert LabVIEW timestamp to datetime object with timezone."""
	return LABVIEW_EPOCH + timedelta(seconds=ts) + timedelta(hours=tz_offset_hours)


@dataclass
class Channel:
	datatype: int
	name: str
	unit: str
	tag: str
	offset: int


def read_u32(f) -> int:
	b = f.read(4)
	if len(b) != 4:
		raise EOFError('Unexpected EOF reading U32')
	return struct.unpack('<I', b)[0]


def read_i32(f) -> int:
	b = f.read(4)
	if len(b) != 4:
		raise EOFError('Unexpected EOF reading I32')
	return struct.unpack('<i', b)[0]


def read_string(f) -> str:
	length = read_u32(f)
	if length == 0:
		return ''
	b = f.read(length)
	if len(b) != length:
		raise EOFError('Unexpected EOF reading string bytes')
	try:
		return b.decode('utf-8', errors='replace')
	except Exception:
		return b.decode('latin-1', errors='replace')


def read_header(f) -> List[Channel]:
	format_id = read_i32(f)
	if format_id != 117431:
		raise ValueError(f'Unexpected format id: {format_id}')
	count = read_u32(f)
	channels: List[Channel] = []
	for _ in range(count):
		datatype = read_i32(f)
		name = read_string(f)
		unit = read_string(f)
		tag = read_string(f)
		offset = read_i32(f)
		channels.append(Channel(datatype, name, unit, tag, offset))
	return channels


def read_double_vector(f, offset: int) -> Tuple[List[float], int]:
	f.seek(offset)
	n = read_u32(f)
	vals: List[float] = []
	for _ in range(n):
		b = f.read(8)
		if len(b) != 8:
			raise EOFError('Unexpected EOF reading double vector')
		vals.append(struct.unpack('<d', b)[0])
	return vals, f.tell()


def read_channel_series(f, channel: Channel) -> Tuple[List[float], List[float]]:
	# At offset: U32 len; len*doubles time, then U32 len; len*doubles values
	times, pos_after_time = read_double_vector(f, channel.offset)
	vals, _ = read_double_vector(f, pos_after_time)
	return times, vals


def write_parquet(out_path: str, timestamps: List[float], end_cast_vals: List[float], tz_offset_hours: int = 0):
	os.makedirs(os.path.dirname(out_path), exist_ok=True)
	
	# Convert timestamps to proper datetime objects first
	datetime_objects = []
	for ts in timestamps:
		dt = labview_to_datetime(ts, tz_offset_hours=tz_offset_hours)
		datetime_objects.append(dt)
	
	# Create DataFrame with proper datetime column
	df = pd.DataFrame({
		'datetime': datetime_objects,
		'end_of_cast': end_cast_vals[:len(datetime_objects)]
	})
	
	# Ensure datetime column is properly formatted
	df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
	
	# Save to Parquet (much faster and smaller than CSV)
	df.to_parquet(out_path, index=False, engine='pyarrow')


def find_channel(channels: List[Channel], substrings: List[str]) -> Optional[Channel]:
	lowers = [(c, (c.name + ' ' + c.tag + ' ' + c.unit).lower()) for c in channels]
	for c, s in lowers:
		if all(sub.lower() in s for sub in substrings):
			return c
	return None


def main(argv: List[str]) -> int:
	ap = argparse.ArgumentParser(description='Extract end_of_cast from .OPCB files (time/value vectors)')
	ap.add_argument('input_file')
	ap.add_argument('--out', default=None)
	args = ap.parse_args(argv)

	with open(args.input_file, 'rb') as f:
		channels = read_header(f)
		# Find end of cast channel
		ch_end_cast = find_channel(channels, ['end of cast']) or find_channel(channels, ['end']) or find_channel(channels, ['cast'])

		if not ch_end_cast:
			raise RuntimeError('end_of_cast channel not found in header')

		times, vals = read_channel_series(f, ch_end_cast)
		base = os.path.splitext(os.path.basename(args.input_file))[0]
		out = args.out or os.path.join('.', 'parsed_files', 'OPCB', f'{base}_endofcast.parquet')
		# Bake in +2 hour local offset
		write_parquet(out, times, vals, tz_offset_hours=2)
		
		# Create/update metadata for OPCB files
		output_dir = Path('./parsed_files/OPCB')
		try:
			from create_metadata_utils import update_parser_metadata
			update_parser_metadata(output_dir, 'endofcast', base)
		except Exception as e:
			# Don't fail if metadata creation fails
			pass
		
		print({'status': 'ok', 'out_parquet': out, 'channel': ch_end_cast.name, 'tz_offset_hours': 2})
		return 0


if __name__ == '__main__':
	raise SystemExit(main(sys.argv[1:]))
