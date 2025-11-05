"""
OPCB Channel List Generator


This script uses the same format as existing OPCB parsers to list all available channels.
"""

import os
import sys
import struct
import argparse
from dataclasses import dataclass
from typing import List


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
    """Read channel definitions using the same method as existing parsers."""
    format_id = read_i32(f)
    if format_id != 117431:
        raise ValueError(f'Unexpected format id: {format_id}')
    
    count = read_u32(f)
    print(f"Format ID: {format_id}")
    print(f"Number of channels: {count}")
    
    channels: List[Channel] = []
    for i in range(count):
        try:
            datatype = read_i32(f)
            name = read_string(f)
            unit = read_string(f)
            tag = read_string(f)
            offset = read_i32(f)
            channels.append(Channel(datatype, name, unit, tag, offset))
            
            # Print progress every 100 channels
            if (i + 1) % 100 == 0:
                print(f"Processed {i+1} channels...")
                
        except Exception as e:
            print(f"Error reading channel {i}: {e}")
            break
    
    return channels


def generate_markdown_report(channels: List[Channel], output_file: str):
    """Generate a comprehensive markdown report."""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# OPCB Channel Analysis Report\n\n")
        f.write(f"**Total Channels Found:** {len(channels)}\n\n")
        
        # Categorize channels
        temp_channels = []
        pressure_channels = []
        flow_channels = []
        position_channels = []
        control_channels = []
        status_channels = []
        process_channels = []
        other_channels = []
        
        for channel in channels:
            name_lower = channel.name.lower()
            if any(keyword in name_lower for keyword in ['temp', 'temperature', 'thermal', 'delta']):
                temp_channels.append(channel)
            elif any(keyword in name_lower for keyword in ['pressure', 'press', 'bar']):
                pressure_channels.append(channel)
            elif any(keyword in name_lower for keyword in ['flow', 'rate', 'speed', 'velocity']):
                flow_channels.append(channel)
            elif any(keyword in name_lower for keyword in ['position', 'pos', 'level', 'height', 'length', 'depth']):
                position_channels.append(channel)
            elif any(keyword in name_lower for keyword in ['control', 'setpoint', 'sp', 'pid', 'valve', 'pump', 'program']):
                control_channels.append(channel)
            elif any(keyword in name_lower for keyword in ['status', 'alarm', 'fault', 'error', 'warning', 'state', 'goodness']):
                status_channels.append(channel)
            elif any(keyword in name_lower for keyword in ['cast', 'steel', 'mold', 'tundish', 'osc', 'friction', 'stopper']):
                process_channels.append(channel)
            else:
                other_channels.append(channel)
        
        # Write categorized sections
        if temp_channels:
            f.write("## 🌡️ Temperature Channels\n\n")
            for channel in temp_channels:
                f.write(f"- **{channel.name}** ({channel.unit}) - {channel.tag}\n")
            f.write("\n")
        
        if pressure_channels:
            f.write("## 🔧 Pressure Channels\n\n")
            for channel in pressure_channels:
                f.write(f"- **{channel.name}** ({channel.unit}) - {channel.tag}\n")
            f.write("\n")
        
        if flow_channels:
            f.write("## 💧 Flow/Rate Channels\n\n")
            for channel in flow_channels:
                f.write(f"- **{channel.name}** ({channel.unit}) - {channel.tag}\n")
            f.write("\n")
        
        if position_channels:
            f.write("## 📏 Position/Level Channels\n\n")
            for channel in position_channels:
                f.write(f"- **{channel.name}** ({channel.unit}) - {channel.tag}\n")
            f.write("\n")
        
        if control_channels:
            f.write("## 🎛️ Control Channels\n\n")
            for channel in control_channels:
                f.write(f"- **{channel.name}** ({channel.unit}) - {channel.tag}\n")
            f.write("\n")
        
        if status_channels:
            f.write("## ⚠️ Status/Alarm Channels\n\n")
            for channel in status_channels:
                f.write(f"- **{channel.name}** ({channel.unit}) - {channel.tag}\n")
            f.write("\n")
        
        if process_channels:
            f.write("## 🏭 Process Channels\n\n")
            for channel in process_channels:
                f.write(f"- **{channel.name}** ({channel.unit}) - {channel.tag}\n")
            f.write("\n")
        
        if other_channels:
            f.write("## 📋 Other Channels\n\n")
            for channel in other_channels:
                f.write(f"- **{channel.name}** ({channel.unit}) - {channel.tag}\n")
            f.write("\n")
        
        # Summary table
        f.write("## 📊 Channel Summary\n\n")
        f.write("| Category | Count |\n")
        f.write("|----------|-------|\n")
        f.write(f"| Temperature | {len(temp_channels)} |\n")
        f.write(f"| Pressure | {len(pressure_channels)} |\n")
        f.write(f"| Flow/Rate | {len(flow_channels)} |\n")
        f.write(f"| Position/Level | {len(position_channels)} |\n")
        f.write(f"| Control | {len(control_channels)} |\n")
        f.write(f"| Status/Alarm | {len(status_channels)} |\n")
        f.write(f"| Process | {len(process_channels)} |\n")
        f.write(f"| Other | {len(other_channels)} |\n")
        f.write(f"| **Total** | **{len(channels)}** |\n")


def main():
    """Main function to scan OPCB file."""
    parser = argparse.ArgumentParser(description='OPCB channel list generator')
    parser.add_argument('input_file', help='Path to .OPCB file')
    parser.add_argument('--out', help='Output markdown file', default='opcb_channels_report.md')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"Error: File {args.input_file} not found")
        return 1
    
    print(f"Scanning OPCB file: {args.input_file}")
    
    try:
        with open(args.input_file, 'rb') as f:
            channels = read_header(f)
            print(f"Successfully processed {len(channels)} channels")
            
            # Generate markdown report
            output_path = args.out if os.path.isabs(args.out) else os.path.join('', args.out)
            generate_markdown_report(channels, output_path)
            print(f"Report generated: {output_path}")
            
            return 0
            
    except Exception as e:
        print(f"Error processing file: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
