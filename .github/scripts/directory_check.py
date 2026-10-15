#!/usr/bin/env python3
import os
import sys
import json
import re
from urllib.parse import urlparse

FORBIDDEN_KEYWORDS = [
    "go_sdk",
    "python_sdk",
    "ekuiper-manager",
    "legacy_backup",
    "Dockerfile-alpine-python",
    "Dockerfile-slim-python",
    "openvino",
    "tensorflow_lite",
    "lf edge",
    "lf-edge",
    "lfedge",
]

success = True


def normalize_path(path):
    return os.path.normpath(path).replace('\\', '/')


def get_md_files_from_config(dir_config, docs_path, lang_dir):
    md_list = []
    for item in dir_config:
        md_name = item.get('path')
        md_children = item.get('children')

        if md_name:
            if md_name.startswith(('http://', 'https://')):
                continue
            elif md_name in ('.', './'):
                target = os.path.join(docs_path, lang_dir, 'index.md')
                md_list.append(normalize_path(target))
            else:
                target = os.path.join(docs_path, lang_dir, f'{md_name}.md')
                md_list.append(normalize_path(target))

        if md_children:
            md_list += get_md_files_from_config(md_children, docs_path, lang_dir)

    return list(set(md_list))


def check_forbidden_keywords(md_file, content):
    global success
    for line_idx, line in enumerate(content.splitlines(), start=1):
        for keyword in FORBIDDEN_KEYWORDS:
            if re.search(r'\b' + re.escape(keyword) + r'\b', line, re.IGNORECASE) or keyword in line.lower():
                print(f"[FORBIDDEN KEYWORD] {md_file}:{line_idx}: contains banned term '{keyword}': {line.strip()}")
                success = False


def check_relative_links_and_images(md_file, content, docs_path):
    global success
    base_dir = os.path.dirname(md_file)

    # Strip code blocks to avoid false positives in code snippets
    # but keep markdown link syntax outside code blocks
    cleaned_content = re.sub(r'```[\s\S]*?```', '', content)
    cleaned_content = re.sub(r'<!--[\s\S]*?-->', '', cleaned_content)

    # Find all image links: ![alt](target) and <img src="..." />
    image_matches = re.findall(r'!\[(.*?)\]\((.*?)\)', cleaned_content)
    raw_targets = [target for _, target in image_matches]
    raw_targets += re.findall(r'<img\s+[^>]*src=["\'](.*?)["\']', cleaned_content, re.IGNORECASE)

    for target in raw_targets:
        target = target.strip()
        if not target or target.startswith(('http://', 'https://', 'data:', '<')):
            continue
        # Strip title attribute if present: "url 'title'"
        clean_target = target.split()[0].strip('\'"')
        clean_target = clean_target.split('#')[0].split('?')[0]
        if not clean_target:
            continue

        if clean_target.startswith('/'):
            rel_target = clean_target.lstrip('/')
            candidates = [
                os.path.join(docs_path, 'en_US', 'public', rel_target),
                os.path.join(docs_path, 'zh_CN', 'public', rel_target),
                os.path.join(docs_path, 'public', rel_target),
                os.path.join(docs_path, rel_target),
            ]
            if any(os.path.exists(p) for p in candidates):
                continue
            print(f"[MISSING IMAGE] {md_file}: Image target '{target}' does not exist in public directories")
            success = False
            continue

        resolved = normalize_path(os.path.join(base_dir, clean_target))
        if not os.path.exists(resolved):
            print(f"[MISSING IMAGE] {md_file}: Image target '{target}' does not exist on disk (resolved to: {resolved})")
            success = False

    # Find all relative markdown links: [text](target)
    link_matches = re.findall(r'(?<!\!)\[(.*?)\]\((.*?)\)', cleaned_content)
    for text, target in link_matches:
        target = target.strip()
        if not target or target.startswith(('http://', 'https://', '#', 'mailto:', 'tel:', '<')):
            continue
        if target.endswith('.html') or '.html#' in target:
            continue
        # Strip title attribute if present
        clean_target = target.split()[0].strip('\'"')
        clean_target = clean_target.split('#')[0].split('?')[0]
        if not clean_target:
            continue

        if clean_target.startswith('/'):
            rel_target = clean_target.lstrip('/')
            candidates = [
                os.path.join(docs_path, 'en_US', rel_target),
                os.path.join(docs_path, 'zh_CN', rel_target),
                os.path.join(docs_path, rel_target),
            ]
            found = False
            for p in candidates:
                if os.path.exists(p) or (not p.endswith('.md') and os.path.exists(p + '.md')):
                    found = True
                    break
            if found:
                continue
            print(f"[BROKEN LINK] {md_file}: Root link '{target}' does not exist on disk")
            success = False
            continue

        # Check if local file exists
        resolved = normalize_path(os.path.join(base_dir, clean_target))
        if not os.path.exists(resolved):
            # If target didn't have .md, check with .md extension
            if not resolved.endswith('.md') and os.path.exists(resolved + '.md'):
                continue
            print(f"[BROKEN LINK] {md_file}: Relative link '{target}' does not exist on disk (resolved to: {resolved})")
            success = False


def main():
    global success
    if len(sys.argv) < 3:
        print("Usage: directory_check.py <directory_file> <docs_path>")
        sys.exit(1)

    directory_file = sys.argv[1]
    docs_path = sys.argv[2]

    # Resolve config file path
    if os.path.exists(directory_file):
        config_path = directory_file
    elif os.path.exists(os.path.join(docs_path, directory_file)):
        config_path = os.path.join(docs_path, directory_file)
    else:
        print(f"Error: directory configuration file '{directory_file}' not found.")
        sys.exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        config_dict = json.load(f)

    # Verify all files listed in directory.json exist
    expected_files = []
    if 'en' in config_dict:
        expected_files += get_md_files_from_config(config_dict['en'], docs_path, 'en_US')
    if 'cn' in config_dict:
        expected_files += get_md_files_from_config(config_dict['cn'], docs_path, 'zh_CN')

    for expected_file in expected_files:
        if not os.path.exists(expected_file):
            print(f"[MISSING DOCUMENT] Configured page '{expected_file}' listed in {config_path} does not exist.")
            success = False

    # Scan all markdown files in docs/en_US
    scan_root = os.path.join(docs_path, 'en_US')
    if not os.path.exists(scan_root):
        scan_root = docs_path

    all_scanned_files = []
    for root, dirs, files in os.walk(scan_root):
        # Skip node_modules and .vitepress build artifacts
        if 'node_modules' in root or '.vitepress' in root or 'dist' in root or 'legacy_backup' in root:
            continue
        for file in files:
            if file.endswith('.md'):
                all_scanned_files.append(normalize_path(os.path.join(root, file)))

    print(f"Auditing {len(all_scanned_files)} markdown files in {scan_root}...")

    for md_file in all_scanned_files:
        try:
            with open(md_file, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
        except Exception as e:
            print(f"[READ ERROR] Could not read {md_file}: {e}")
            success = False
            continue

        check_forbidden_keywords(md_file, content)
        check_relative_links_and_images(md_file, content, docs_path)

    if not success:
        sys.exit("Directory & link integrity check FAILED! Please resolve the errors above.")
    else:
        print("Directory check completed successfully! (0 forbidden keywords, 0 broken links/images).")


if __name__ == '__main__':
    main()
