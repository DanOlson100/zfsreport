#!/usr/bin/env python3
"""
ZFS Statistics Email Reporter
Collects ZFS pool health, usage, and scrub status and emails a report
"""

import subprocess
import smtplib
import json
import re
import yaml
import os
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional

class ZFSReporter:
    def __init__(self, config_file: str = 'zfs_config.yaml'):
        """Initialize with config from YAML file"""
        self.config = self.load_config(config_file)

        # Extract email settings
        email_config = self.config['email']
        self.smtp_server = email_config['smtp_server']
        self.smtp_port = email_config['smtp_port']
        self.email_user = email_config['username']
        self.email_pass = email_config['password']
        self.from_addr = email_config['from_address']
        self.to_addrs = email_config['to_addresses']

        # Extract alert thresholds
        alerts = self.config.get('alert_thresholds', {})
        self.capacity_warning = alerts.get('capacity_warning', 80)
        self.capacity_critical = alerts.get('capacity_critical', 90)
        self.scrub_warning_days = alerts.get('scrub_warning_days', 30)
        self.scrub_critical_days = alerts.get('scrub_critical_days', 90)
        self.error_warning = alerts.get('error_warning', 1)
        self.error_critical = alerts.get('error_critical', 10)

    def load_config(self, config_file: str) -> Dict:
        """Load configuration from YAML file"""
        if not os.path.exists(config_file):
            self.create_sample_config(config_file)
            print(f"📝 Created sample config file: {config_file}")
            print("Please edit the config file with your settings and run again.")
            exit(1)

        try:
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"❌ Error loading config file {config_file}: {e}")
            exit(1)

    def create_sample_config(self, config_file: str):
        """Create a sample configuration file"""
        sample_config = {
            'email': {
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'username': 'your-email@gmail.com',
                'password': 'your-app-password',
                'from_address': 'your-email@gmail.com',
                'to_addresses': ['admin@yourcompany.com', 'backup-admin@yourcompany.com']
            },
            'alert_thresholds': {
                'capacity_warning': 80,
                'capacity_critical': 90,
                'scrub_warning_days': 30,
                'scrub_critical_days': 90,
                'error_warning': 1,
                'error_critical': 10
            },
            'report_settings': {
                'include_healthy_pools': True,
                'email_on_success': True,
                'email_on_warnings_only': False
            }
        }

        with open(config_file, 'w') as f:
            yaml.dump(sample_config, f, default_flow_style=False, indent=2)

    def run_zfs_command(self, cmd: List[str]) -> str:
        """Run a ZFS command and return output"""
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            return f"Error running {' '.join(cmd)}: {e.stderr}"

    def get_pool_health(self) -> Dict[str, str]:
        """Get health status for all pools"""
        pools = {}
        output = self.run_zfs_command(['zpool', 'list', '-H', '-o', 'name,health'])

        for line in output.split('\n'):
            if line.strip():
                parts = line.split('\t')
                if len(parts) >= 2:
                    pools[parts[0]] = parts[1]

        return pools

    def get_pool_usage(self) -> Dict[str, Dict[str, str]]:
        """Get usage statistics for all pools"""
        pools = {}
        output = self.run_zfs_command(['zpool', 'list', '-H', '-o', 'name,size,alloc,free,cap'])

        for line in output.split('\n'):
            if line.strip():
                parts = line.split('\t')
                if len(parts) >= 5:
                    pools[parts[0]] = {
                        'size': parts[1],
                        'allocated': parts[2],
                        'free': parts[3],
                        'capacity': parts[4]
                    }

        return pools

    def get_pool_errors(self) -> Dict[str, Dict[str, str]]:
        """Get read, write, and checksum errors for all pools"""
        pools = {}
        output = self.run_zfs_command(['zpool', 'status'])

        current_pool = None
        for line in output.split('\n'):
            line = line.strip()

            # Find pool name
            if line.startswith('pool:'):
                current_pool = line.split(':', 1)[1].strip()
                pools[current_pool] = {'read': '0', 'write': '0', 'cksum': '0'}

            # Look for error summary line (usually contains pool name and error counts)
            elif current_pool and current_pool in line and not line.startswith('config:'):
                # Try to extract error numbers from the line
                parts = line.split()
                if len(parts) >= 4:
                    # Usually format: poolname ONLINE 0 0 0
                    try:
                        pools[current_pool] = {
                            'read': parts[-3] if parts[-3].isdigit() else '0',
                            'write': parts[-2] if parts[-2].isdigit() else '0', 
                            'cksum': parts[-1] if parts[-1].isdigit() else '0'
                        }
                    except (IndexError, ValueError):
                        pools[current_pool] = {'read': '0', 'write': '0', 'cksum': '0'}

        return pools

    def get_scrub_status(self) -> Dict[str, Dict[str, str]]:
        """Get scrub status for all pools"""
        pools = {}
        pool_names = self.run_zfs_command(['zpool', 'list', '-H', '-o', 'name']).split('\n')

        for pool in pool_names:
            if pool.strip():
                status_output = self.run_zfs_command(['zpool', 'status', pool.strip()])

                # Parse scrub info from status output
                scrub_data = {
                    'status': 'No scrub info found',
                    'last_scrub': 'Never',
                    'days_ago': 'N/A'
                }

                lines = status_output.split('\n')

                for i, line in enumerate(lines):
                    if 'scan:' in line.lower() or 'scrub:' in line.lower():
                        full_scrub_line = line.strip()
                        # Sometimes scrub info spans multiple lines
                        if i + 1 < len(lines) and lines[i + 1].strip().startswith(' '):
                            full_scrub_line += ' ' + lines[i + 1].strip()

                        scrub_data['status'] = full_scrub_line

                        # Extract date from scrub line
                        # Look for patterns like "on Sun Dec 15 14:30:25 2024"
                        import re
                        date_match = re.search(r'on\s+\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4}', full_scrub_line)

                        if date_match:
                            date_str = date_match.group().replace('on ', '')
                            try:
                                scrub_date = datetime.strptime(date_str, '%a %b %d %H:%M:%S %Y')
                                days_diff = (datetime.now() - scrub_date).days

                                scrub_data['last_scrub'] = scrub_date.strftime('%Y-%m-%d %H:%M')
                                scrub_data['days_ago'] = str(days_diff)
                            except ValueError:
                                # If parsing fails, keep defaults
                                pass

                        break

                pools[pool.strip()] = scrub_data

        return pools

    def format_table(self, headers: List[str], rows: List[List[str]], title: str = "") -> str:
        """Format data as an ASCII table"""
        if not rows:
            return f"{title}\nNo data available\n"

        # Calculate column widths
        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, cell in enumerate(row):
                if i < len(col_widths):
                    col_widths[i] = max(col_widths[i], len(str(cell)))

        # Create table
        table = ""
        if title:
            table += f"{title}\n"

        # Header row
        header_row = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
        table += header_row + "\n"

        # Separator
        separator = "|" + "|".join("-" * (w + 2) for w in col_widths) + "|"
        table += separator + "\n"

        # Data rows
        for row in rows:
            data_row = "| " + " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)) + " |"
            table += data_row + "\n"

        return table + "\n"
        """Get scrub status for all pools"""
        pools = {}
        pool_names = self.run_zfs_command(['zpool', 'list', '-H', '-o', 'name']).split('\n')

        for pool in pool_names:
            if pool.strip():
                status_output = self.run_zfs_command(['zpool', 'status', pool.strip()])

                # Parse scrub info from status output
                scrub_info = "No scrub info found"
                lines = status_output.split('\n')

                for i, line in enumerate(lines):
                    if 'scan:' in line.lower() or 'scrub:' in line.lower():
                        scrub_info = line.strip()
                        # Sometimes scrub info spans multiple lines
                        if i + 1 < len(lines) and lines[i + 1].strip().startswith(' '):
                            scrub_info += ' ' + lines[i + 1].strip()
                        break

                pools[pool.strip()] = scrub_info

        return pools

    def generate_report(self) -> str:
        """Generate the full ZFS report"""
        report_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Collect all data
        pool_health = self.get_pool_health()
        pool_usage = self.get_pool_usage()
        pool_errors = self.get_pool_errors()
        scrub_status = self.get_scrub_status()

        # Build report
        report = f"""ZFS Status Report - {report_time}
{'='*120}

"""

        # Combined Pool Status Table
        headers = [
            "Pool", "Health", "Size", "Used", "Free", "Capacity", 
            "Read Err", "Write Err", "Cksum Err", "Last Scrub", "Days Ago", "Status"
        ]
        rows = []

        for pool in pool_health.keys():
            health = pool_health.get(pool, "UNKNOWN")
            usage = pool_usage.get(pool, {})
            errors = pool_errors.get(pool, {'read': '0', 'write': '0', 'cksum': '0'})
            scrub = scrub_status.get(pool, {'last_scrub': 'Never', 'days_ago': 'N/A'})

            # Calculate status icons
            health_ok = health == "ONLINE"

            read_errs = int(errors.get('read', '0'))
            write_errs = int(errors.get('write', '0'))
            cksum_errs = int(errors.get('cksum', '0'))
            total_errs = read_errs + write_errs + cksum_errs
            errors_ok = total_errs == 0

            # Scrub age warning (warn if > threshold days)
            days_ago_str = scrub.get('days_ago', 'N/A')
            scrub_ok = True
            if days_ago_str.isdigit():
                days_ago = int(days_ago_str)
                scrub_ok = days_ago <= self.scrub_warning_days

            # Overall status
            if health_ok and errors_ok and scrub_ok:
                overall_status = "✅"
            elif not health_ok or total_errs > self.error_critical:
                overall_status = "❌"
            else:
                overall_status = "⚠️"

            # Format capacity with warning colors
            capacity = usage.get('capacity', 'N/A')
            if capacity != 'N/A' and capacity.rstrip('%').isdigit():
                cap_pct = int(capacity.rstrip('%'))
                if cap_pct >= self.capacity_critical:
                    capacity = f"{capacity} ❌"
                elif cap_pct >= self.capacity_warning:
                    capacity = f"{capacity} ⚠️"

            # Format days ago with warning
            days_display = days_ago_str
            if days_ago_str.isdigit():
                days = int(days_ago_str)
                if days > self.scrub_critical_days:
                    days_display = f"{days} ❌"
                elif days > self.scrub_warning_days:
                    days_display = f"{days} ⚠️"

            row = [
                pool,
                health,
                usage.get('size', 'N/A'),
                usage.get('allocated', 'N/A'),
                usage.get('free', 'N/A'),
                capacity,
                str(read_errs),
                str(write_errs),
                str(cksum_errs),
                scrub.get('last_scrub', 'Never'),
                days_display,
                overall_status
            ]
            rows.append(row)

        report += self.format_table(headers, rows, "ZFS POOL STATUS:")

        # Summary
        total_pools = len(pool_health)
        healthy_pools = sum(1 for h in pool_health.values() if h == "ONLINE")
        total_errors = sum(
            int(errors.get('read', '0')) + int(errors.get('write', '0')) + int(errors.get('cksum', '0'))
            for errors in pool_errors.values()
        )

        # Count pools with old scrubs
        old_scrubs = 0
        for pool_scrub in scrub_status.values():
            days_str = pool_scrub.get('days_ago', 'N/A')
            if days_str.isdigit() and int(days_str) > self.scrub_warning_days:
                old_scrubs += 1

        report += f"SUMMARY:\n{'-'*60}\n"
        report += f"Total Pools: {total_pools}\n"
        report += f"Healthy Pools: {healthy_pools}/{total_pools}\n"
        report += f"Total Errors: {total_errors}\n"
        report += f"Pools with old scrubs (>{self.scrub_warning_days} days): {old_scrubs}\n\n"

        # Warnings
        warnings = []
        if healthy_pools < total_pools:
            warnings.append(f"{total_pools - healthy_pools} pool(s) are not ONLINE")
        if total_errors > 0:
            warnings.append(f"{total_errors} total errors detected")
        if old_scrubs > 0:
            warnings.append(f"{old_scrubs} pool(s) need scrubbing")

        if warnings:
            report += "⚠️  WARNINGS:\n"
            for warning in warnings:
                report += f"   • {warning}\n"
        else:
            report += "✅ All pools are healthy with no errors and recent scrubs!\n"

        return report

    def send_email(self, subject: str, body: str):
        """Send email report"""
        msg = MIMEMultipart()
        msg['From'] = self.from_addr
        msg['To'] = ', '.join(self.to_addrs)
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.email_user, self.email_pass)
            server.send_message(msg)
            server.quit()
            print("✅ Email sent successfully!")
        except Exception as e:
            print(f"❌ Failed to send email: {e}")

    def run_report(self):
        """Generate and send the ZFS report"""
        print("🔄 Generating ZFS report...")
        report = self.generate_report()

        # Determine subject based on health
        pool_health = self.get_pool_health()
        pool_errors = self.get_pool_errors()
        scrub_status = self.get_scrub_status()

        healthy_pools = sum(1 for h in pool_health.values() if h == "ONLINE")
        total_pools = len(pool_health)
        total_errors = sum(
            int(errors.get('read', '0')) + int(errors.get('write', '0')) + int(errors.get('cksum', '0'))
            for errors in pool_errors.values()
        )

        # Count pools with old scrubs
        old_scrubs = sum(1 for scrub in scrub_status.values() 
                        if scrub.get('days_ago', 'N/A').isdigit() and int(scrub['days_ago']) > self.scrub_warning_days)

        if healthy_pools == total_pools and total_errors == 0 and old_scrubs == 0:
            subject = f"✅ ZFS Report - All {total_pools} pools healthy"
        elif healthy_pools < total_pools:
            subject = f"❌ ZFS Report - {total_pools - healthy_pools} pool(s) unhealthy"
        elif total_errors > 0:
            subject = f"⚠️ ZFS Report - {total_errors} errors detected"
        elif old_scrubs > 0:
            subject = f"⚠️ ZFS Report - {old_scrubs} pool(s) need scrubbing"
        else:
            subject = f"✅ ZFS Report - All systems normal"

        print("📧 Sending email...")
        self.send_email(subject, report)

        # Also print to console for debugging
        print("\n" + "="*50)
        print("REPORT PREVIEW:")
        print("="*50)
        print(report)

def main():

    # Create reporter and run
    reporter = ZFSReporter()
    reporter.run_report()

if __name__ == "__main__":
    main()

